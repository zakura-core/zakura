use super::*;

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
