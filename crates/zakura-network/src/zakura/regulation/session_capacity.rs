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

use std::sync::Arc;

use zakura_rt::sync::{watch, Mutex, OwnedSemaphorePermit, PoisonError, Semaphore};

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
    #[cfg(zakura_sim)]
    owners: Arc<ReservationOwners>,
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
            #[cfg(zakura_sim)]
            owners: Arc::default(),
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
        #[cfg(zakura_sim)]
        let _transition = self.owners.transition();
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
        let reservation = Arc::new(SessionReservation {
            reserved,
            setting_up,
            session: Some(session),
            setup: Mutex::new(Some(setup)),
            changed: self.changed.clone(),
            #[cfg(zakura_sim)]
            owners: self.owners.clone(),
            #[cfg(zakura_sim)]
            direction,
            #[cfg(zakura_sim)]
            observation_id: self
                .owners
                .next
                .fetch_add(1, zakura_rt::sync::counter::Ordering::Relaxed),
        });
        #[cfg(zakura_sim)]
        {
            let mut owners = self.owners.reservations.lock().unwrap();
            owners.retain(|owner| owner.strong_count() > 0);
            owners.push(Arc::downgrade(&reservation));
        }
        Ok(reservation)
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
    #[cfg(zakura_sim)]
    owners: Arc<ReservationOwners>,
    #[cfg(zakura_sim)]
    direction: ServicePeerDirection,
    #[cfg(zakura_sim)]
    observation_id: usize,
}

impl SessionResources for SessionReservation {
    /// Release the setup slots once every member is ready. Later calls
    /// release nothing.
    fn admitted(&self) {
        #[cfg(zakura_sim)]
        let _transition = self.owners.transition();
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
        #[cfg(zakura_sim)]
        let _transition = self.owners.transition();
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

/// Free slots of each kind.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FreeSlots {
    /// Free inbound direction slots.
    pub inbound: usize,
    /// Free outbound direction slots.
    pub outbound: usize,
    /// Free setup slots.
    pub setup: usize,
    /// Free setup slots that inbound sessions may take.
    pub inbound_setup: usize,
}

/// A read of a [`SessionCapacity`]'s free slots for the simulator's
/// harness, which keeps it past the capacity.
///
/// It holds the semaphores and weak reservation references, so it keeps no
/// service or waiter alive. Every reservation holds the same semaphores, so
/// a read once the service is gone still sees a slot a stray reservation
/// holds.
#[cfg(zakura_sim)]
#[derive(Clone, Debug)]
pub struct SessionSlotsProbe {
    owners: Arc<ReservationOwners>,
    inbound: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    setup: Arc<Semaphore>,
    inbound_setup: Arc<Semaphore>,
}

#[cfg(zakura_sim)]
impl SessionSlotsProbe {
    /// The free slots of each kind, read through peeks, so the read adds no
    /// scheduling point.
    ///
    /// # Panics
    ///
    /// Outside a [`boundary_read`](zakura_rt::sim::boundary_read).
    pub fn read(&self) -> FreeSlots {
        assert!(
            zakura_rt::sim::in_boundary_read(),
            "a session slots probe reads only inside a boundary read"
        );
        FreeSlots {
            inbound: self.inbound.peek_available(),
            outbound: self.outbound.peek_available(),
            setup: self.setup.peek_available(),
            inbound_setup: self.inbound_setup.peek_available(),
        }
    }
}

#[cfg(zakura_sim)]
impl SessionCapacity {
    /// A probe of these slots for the simulator's harness.
    pub(crate) fn probe(&self) -> SessionSlotsProbe {
        SessionSlotsProbe {
            owners: self.owners.clone(),
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            setup: self.setup.clone(),
            inbound_setup: self.inbound_setup.clone(),
        }
    }
}

/// A stable comparison of live reservations with the actual free permits.
#[cfg(zakura_sim)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionOwnership {
    /// Permits currently available to new sessions.
    pub free: FreeSlots,
    /// Permits required by reservations that still have at least one owner.
    pub held: FreeSlots,
    /// Inbound reservations that still hold setup permits. IDs never repeat within this capacity.
    pub inbound_setups: Vec<usize>,
}

#[cfg(zakura_sim)]
impl SessionSlotsProbe {
    /// Count actual reservations, including reservations retained after retirement.
    /// Return `None` while a reservation changes its permits.
    pub fn ownership(&self) -> Option<SessionOwnership> {
        use zakura_rt::{sim::Peek, sync::counter::Ordering};
        let free = self.read();
        if self.owners.transitions.load(Ordering::Relaxed) != 0 {
            return None;
        }
        let mut inbound_setups = Vec::new();
        let mut held = FreeSlots {
            inbound: 0,
            outbound: 0,
            setup: 0,
            inbound_setup: 0,
        };
        for owner in self.owners.reservations.lock().unwrap().iter() {
            let Some(owner) = owner.upgrade() else {
                continue;
            };
            let inbound = owner.direction == ServicePeerDirection::Inbound;
            if inbound {
                held.inbound += 1;
            } else {
                held.outbound += 1;
            }
            let setting_up = match owner.setup.peek(Option::is_some) {
                Peek::Current(value) | Peek::Poisoned(value) => value,
                Peek::Held { .. } => return None,
            };
            if setting_up {
                held.setup += 1;
                held.inbound_setup += usize::from(inbound);
                if inbound {
                    inbound_setups.push(owner.observation_id);
                }
            }
        }
        Some(SessionOwnership {
            free,
            held,
            inbound_setups,
        })
    }
}

// This registry observes ownership without adding simulator scheduling points.
#[cfg(zakura_sim)]
#[derive(Debug, Default)]
struct ReservationOwners {
    reservations: std::sync::Mutex<Vec<std::sync::Weak<SessionReservation>>>,
    transitions: zakura_rt::sync::counter::AtomicUsize,
    next: zakura_rt::sync::counter::AtomicUsize,
}

#[cfg(zakura_sim)]
impl ReservationOwners {
    fn transition(self: &Arc<Self>) -> ReservationTransition {
        self.transitions
            .fetch_add(1, zakura_rt::sync::counter::Ordering::Relaxed);
        ReservationTransition(self.clone())
    }
}

#[cfg(zakura_sim)]
struct ReservationTransition(Arc<ReservationOwners>);

#[cfg(zakura_sim)]
impl Drop for ReservationTransition {
    fn drop(&mut self) {
        self.0
            .transitions
            .fetch_sub(1, zakura_rt::sync::counter::Ordering::Relaxed);
    }
}
