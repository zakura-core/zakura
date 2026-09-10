//! Tracks block-sync session capacity and each peer's current session.
//!
//! A session is the request stream and data stream paired on one QUIC connection.
//! The service reserves capacity during setup and publishes the assembled pair
//! for the reactor (the download coordinator).
//!
//! [`SessionCapacity`] counts pairs during setup, active use, and cleanup.
//! [`CurrentSessions`] holds only the current session for each peer. Replacing
//! its entry doesn't free the old pair's capacity while that pair still has
//! workers or send handles finishing cleanup.

use super::*;
use crate::zakura::{ServicePeerLimits, SessionFull, SessionResources};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Limits session counts by connection direction and bounds incomplete pairs.
///
/// Inbound means the peer connected to us; outbound means we connected to the
/// peer. Each pair takes one slot from its direction and one temporary setup slot
/// from the allowance shared across both directions. When outbound sessions are
/// enabled, inbound setup cannot take the final pending slot. This lets an
/// outbound session start even while inbound peers withhold their second role.
#[derive(Debug)]
pub(super) struct SessionCapacity {
    inbound: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    inbound_pending: Arc<Semaphore>,
    changed: watch::Sender<()>,
}

impl SessionCapacity {
    pub(super) fn new(limits: ServicePeerLimits) -> Self {
        let pool = |count: usize| Arc::new(Semaphore::new(count.min(Semaphore::MAX_PERMITS)));
        let pending = limits.max_pending_escalations.min(Semaphore::MAX_PERMITS);
        let inbound_pending = pending.saturating_sub(usize::from(limits.max_outbound_peers > 0));
        Self {
            inbound: pool(limits.max_inbound_peers),
            outbound: pool(limits.max_outbound_peers),
            pending: pool(pending),
            inbound_pending: pool(inbound_pending),
            changed: watch::channel(()).0,
        }
    }

    fn pool(&self, direction: ServicePeerDirection) -> &Arc<Semaphore> {
        match direction {
            ServicePeerDirection::Inbound => &self.inbound,
            ServicePeerDirection::Outbound => &self.outbound,
        }
    }

    /// Subscribe before checking [`Self::available`], so a capacity release
    /// between the check and waiting for a change cannot be missed.
    pub(super) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Check whether setup could start now, without taking any slots.
    /// Another task can claim them before [`Self::reserve`], so this is only a hint.
    pub(super) fn available(&self, direction: ServicePeerDirection) -> bool {
        self.pool(direction).available_permits() > 0
            && self.pending.available_permits() > 0
            && (direction == ServicePeerDirection::Outbound
                || self.inbound_pending.available_permits() > 0)
    }

    #[cfg(test)]
    pub(super) fn available_counts(&self) -> (usize, usize, usize) {
        (
            self.inbound.available_permits(),
            self.outbound.available_permits(),
            self.pending.available_permits(),
        )
    }

    /// Take one session slot for `direction` and one temporary setup slot.
    ///
    /// Returns [`SessionFull`] immediately if either limit is reached;
    /// an error leaves no slots held by this call. Both streams share the returned
    /// reservation, so opening the second stream must reuse it.
    pub(super) fn reserve(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Arc<dyn SessionResources>, SessionFull> {
        let inbound = if direction == ServicePeerDirection::Inbound {
            Some(
                self.inbound_pending
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| SessionFull)?,
            )
        } else {
            None
        };
        let pending = match self.pending.clone().try_acquire_owned() {
            Ok(node) => PendingSessionSlots {
                _node: node,
                _inbound: inbound,
            },
            Err(_) => {
                if inbound.is_some() {
                    drop(inbound);
                    self.changed.send_replace(());
                }
                return Err(SessionFull);
            }
        };
        let session = match self.pool(direction).clone().try_acquire_owned() {
            Ok(session) => session,
            Err(_) => {
                // Wake callers that may have seen the setup slot occupied.
                drop(pending);
                self.changed.send_replace(());
                return Err(SessionFull);
            }
        };
        let reserved =
            metrics::gauge!("sync.block.sessions.reserved", "direction" => direction.trace_label());
        let pending_count = metrics::gauge!("sync.block.sessions.pending");
        reserved.increment(1.0);
        pending_count.increment(1.0);
        Ok(Arc::new(SessionReservation {
            reserved,
            pending_count,
            session: Some(session),
            pending: StdMutex::new(Some(pending)),
            changed: self.changed.clone(),
        }))
    }
}

/// One pending pair, charged to the node and to the inbound cap when applicable.
#[derive(Debug)]
struct PendingSessionSlots {
    _node: OwnedSemaphorePermit,
    _inbound: Option<OwnedSemaphorePermit>,
}

/// Holds a pair's session and setup slots on behalf of its workers and send handles.
///
/// Each owner retains an `Arc` to this value. The session slot stays occupied
/// after cancellation until the last owner drops its reference.
#[derive(Debug)]
struct SessionReservation {
    reserved: metrics::Gauge,
    pending_count: metrics::Gauge,
    session: Option<OwnedSemaphorePermit>,
    pending: StdMutex<Option<PendingSessionSlots>>,
    changed: watch::Sender<()>,
}

impl SessionResources for SessionReservation {
    /// Release the temporary setup slot once both streams are ready.
    /// The session slot stays reserved; repeated calls cannot release setup twice.
    fn admitted(&self) {
        if self
            .pending
            .lock()
            .expect("session setup ownership is not poisoned")
            .take()
            .is_some()
        {
            self.pending_count.decrement(1.0);
        }
        self.changed.send_replace(());
    }
}

impl Drop for SessionReservation {
    /// Return the session slot and any setup slot left by an incomplete pair,
    /// then wake callers waiting to open a session.
    fn drop(&mut self) {
        if self
            .pending
            .get_mut()
            .expect("session setup ownership is not poisoned")
            .take()
            .is_some()
        {
            self.pending_count.decrement(1.0);
        }
        self.session.take();
        self.reserved.decrement(1.0);
        self.changed.send_replace(());
    }
}

/// The service's current session for each peer, shared with the reactor.
///
/// The service updates the table and calls [`Self::notify`]. The reactor reads a
/// [`Self::snapshot`] and updates its download state to match those sessions.
/// Notifications merge together, so reconnects cannot build a history queue.
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct CurrentSessions {
    pub(super) active: StdMutex<HashMap<ZakuraPeerId, BlockSyncPeerRecord>>,
    changed: watch::Sender<()>,
}

impl CurrentSessions {
    pub(in crate::zakura::block_sync) fn new() -> Arc<Self> {
        Arc::new(Self {
            active: StdMutex::new(HashMap::new()),
            changed: watch::channel(()).0,
        })
    }

    /// Watch for table changes; a notification means to read the current table.
    pub(in crate::zakura::block_sync) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Signal a table change after publishing an updated entry or removal.
    pub(super) fn notify(&self) {
        self.changed.send_replace(());
    }

    /// Clone the current session handles so the reactor can work without the lock.
    ///
    /// Mark the watch notification as seen before taking this snapshot. A change
    /// arriving while the reactor updates its state then triggers another pass.
    pub(in crate::zakura::block_sync) fn snapshot(
        &self,
    ) -> HashMap<ZakuraPeerId, BlockSyncPeerSession> {
        self.active
            .lock()
            .expect("block-sync session table is not poisoned")
            .iter()
            .map(|(peer, record)| (peer.clone(), record.session.clone()))
            .collect()
    }
}
