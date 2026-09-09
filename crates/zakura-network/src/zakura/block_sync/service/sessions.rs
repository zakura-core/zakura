use super::*;
use crate::zakura::{OrderedSessionFull, OrderedSessionResources, ServicePeerLimits};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Setup and retirement retain the same directional session slot. A separate
/// allowance bounds pairs whose second stream has not arrived yet.
#[derive(Debug)]
pub(super) struct SessionCapacity {
    inbound: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    changed: watch::Sender<()>,
}

impl SessionCapacity {
    pub(super) fn new(limits: ServicePeerLimits) -> Self {
        let pool = |count: usize| Arc::new(Semaphore::new(count.min(Semaphore::MAX_PERMITS)));
        Self {
            inbound: pool(limits.max_inbound_peers),
            outbound: pool(limits.max_outbound_peers),
            pending: pool(limits.max_pending_escalations),
            changed: watch::channel(()).0,
        }
    }

    fn pool(&self, direction: ServicePeerDirection) -> &Arc<Semaphore> {
        match direction {
            ServicePeerDirection::Inbound => &self.inbound,
            ServicePeerDirection::Outbound => &self.outbound,
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub(super) fn available(&self, direction: ServicePeerDirection) -> bool {
        self.pool(direction).available_permits() > 0 && self.pending.available_permits() > 0
    }

    #[cfg(test)]
    pub(super) fn available_counts(&self) -> (usize, usize, usize) {
        (
            self.inbound.available_permits(),
            self.outbound.available_permits(),
            self.pending.available_permits(),
        )
    }

    pub(super) fn reserve(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Arc<dyn OrderedSessionResources>, OrderedSessionFull> {
        let pending = self
            .pending
            .clone()
            .try_acquire_owned()
            .map_err(|_| OrderedSessionFull)?;
        let session = match self.pool(direction).clone().try_acquire_owned() {
            Ok(session) => session,
            Err(_) => {
                drop(pending);
                self.changed.send_replace(());
                return Err(OrderedSessionFull);
            }
        };
        let reserved =
            metrics::gauge!("sync.block.sessions.reserved", "direction" => direction.trace_label());
        let pending_count = metrics::gauge!("sync.block.sessions.pending");
        reserved.increment(1.0);
        pending_count.increment(1.0);
        Ok(Arc::new(SessionResources {
            reserved,
            pending_count,
            session: Some(session),
            pending: StdMutex::new(Some(pending)),
            changed: self.changed.clone(),
        }))
    }
}

#[derive(Debug)]
struct SessionResources {
    reserved: metrics::Gauge,
    pending_count: metrics::Gauge,
    session: Option<OwnedSemaphorePermit>,
    pending: StdMutex<Option<OwnedSemaphorePermit>>,
    changed: watch::Sender<()>,
}

impl OrderedSessionResources for SessionResources {
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

impl Drop for SessionResources {
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

/// Authoritative admissions shared by the service and reactor. Notifications
/// carry no history: reconciliation reads a bounded snapshot of current owners.
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

    pub(in crate::zakura::block_sync) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub(super) fn notify(&self) {
        self.changed.send_replace(());
    }

    /// Mark the watch observed before taking this snapshot. A change during
    /// reconciliation then remains pending for the next pass.
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

    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn apply_for_test(
        &self,
        event: BlockSyncPeerLifecycleEvent,
    ) -> Result<(), &'static str> {
        let mut active = self.active.lock().map_err(|_| "session table poisoned")?;
        match event {
            BlockSyncPeerLifecycleEvent::Connected(session) => {
                let peer = session.peer_id().clone();
                if active
                    .get(&peer)
                    .is_some_and(|old| old.session_id >= session.session_id())
                {
                    session.cancel_token().cancel();
                    session.mark_reactor_ready();
                    return Ok(());
                }
                let old = active.insert(
                    peer,
                    BlockSyncPeerRecord {
                        conn_id: 0,
                        session_id: session.session_id(),
                        direction: session.direction(),
                        cancel_token: session.cancel_token(),
                        session,
                    },
                );
                if let Some(old) = old {
                    old.cancel_token.cancel();
                }
            }
            BlockSyncPeerLifecycleEvent::Disconnected { peer, session_id } => {
                if active
                    .get(&peer)
                    .is_some_and(|old| old.session_id == session_id)
                {
                    active.remove(&peer).unwrap().cancel_token.cancel();
                }
            }
        }
        self.notify();
        Ok(())
    }
}
