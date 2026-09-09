use super::*;

#[tokio::test]
async fn pair_setup_and_retirement_keep_their_service_capacity() {
    use crate::zakura::{OrderedSessionResources, ServicePeerLimits};
    let capacity = SessionCapacity::new(ServicePeerLimits {
        max_inbound_peers: 1,
        max_outbound_peers: 1,
        max_pending_escalations: 1,
        ..ServicePeerLimits::default()
    });
    let mut changed = capacity.subscribe();
    let pending = capacity.reserve(ServicePeerDirection::Outbound).unwrap();
    assert!(
        capacity.reserve(ServicePeerDirection::Inbound).is_err(),
        "setup allowance is shared across directions"
    );
    pending.admitted();
    changed.changed().await.unwrap();
    let inbound = capacity.reserve(ServicePeerDirection::Inbound).unwrap();
    inbound.admitted();
    let (send, _recv) = crate::zakura::transport::worker_framed_channel(1);
    let send = send.with_session_resources(Some(pending.clone()));
    let retiring_worker: Arc<dyn OrderedSessionResources> = pending.clone();
    drop(pending);
    assert!(capacity.reserve(ServicePeerDirection::Outbound).is_err());
    drop(send);
    assert!(
        capacity.reserve(ServicePeerDirection::Outbound).is_err(),
        "worker teardown retains its slot after the service drops its sender"
    );
    drop(retiring_worker);
    assert!(capacity.available(ServicePeerDirection::Outbound));
    drop(inbound);
    assert!(capacity.available(ServicePeerDirection::Inbound));
}

#[tokio::test]
async fn abandoned_pair_setup_returns_capacity_and_wakes_demand() {
    use crate::zakura::ServicePeerLimits;
    let capacity = SessionCapacity::new(ServicePeerLimits {
        max_pending_escalations: 1,
        ..ServicePeerLimits::default()
    });
    let mut changed = capacity.subscribe();
    let pending = capacity.reserve(ServicePeerDirection::Outbound).unwrap();
    assert!(!capacity.available(ServicePeerDirection::Inbound));
    drop(pending);
    time::timeout(Duration::from_secs(1), changed.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(capacity.available(ServicePeerDirection::Inbound));
    assert!(capacity.available(ServicePeerDirection::Outbound));
}

#[tokio::test]
async fn session_churn_coalesces_and_changes_during_reconciliation_remain_visible() {
    let current = CurrentSessions::new();
    let mut changed = current.subscribe();
    let peer = ZakuraPeerId::new(vec![211; 32]).unwrap();
    let mut old = Vec::new();
    for id in 1..=1000 {
        let (send, _recv) = crate::zakura::framed_channel(1);
        let cancel = CancellationToken::new();
        current
            .apply_for_test(BlockSyncPeerLifecycleEvent::Connected(
                BlockSyncPeerSession::for_test_with_session_id(
                    peer.clone(),
                    id,
                    send,
                    cancel.clone(),
                ),
            ))
            .unwrap();
        old.push(cancel);
    }
    changed.changed().await.unwrap();
    let snapshot = current.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[&peer].session_id(), 1000);
    assert!(old[..999].iter().all(CancellationToken::is_cancelled));
    assert!(!old[999].is_cancelled());
    assert!(
        !changed.has_changed().unwrap(),
        "one observation consumes the coalesced change"
    );
    current
        .apply_for_test(BlockSyncPeerLifecycleEvent::Disconnected {
            peer,
            session_id: 1000,
        })
        .unwrap();
    assert!(
        changed.has_changed().unwrap(),
        "a change after the snapshot schedules another pass"
    );
    changed.changed().await.unwrap();
    assert!(current.snapshot().is_empty());
}

impl BlockSyncService {
    pub(crate) fn available_session_slots_for_test(&self) -> (usize, usize, usize) {
        self.inner.capacity.available_counts()
    }

    pub(crate) fn sessions_for_transport_test(
        &self,
    ) -> Vec<(u64, BlockSyncPeerSession, FramedSend)> {
        self.inner
            .sessions
            .snapshot()
            .into_values()
            .map(|session| {
                (
                    session.session_id(),
                    session.clone(),
                    session.request_sender(),
                )
            })
            .collect()
    }
}

impl BlockSyncHandle {
    pub(crate) fn active_serving_requests_for_test(&self) -> usize {
        self.routine_wiring
            .as_ref()
            .unwrap()
            .serving_regulator
            .snapshot()
            .node_active
    }

    pub(crate) fn outstanding_requests_for_test(&self) -> usize {
        self.routine_wiring
            .as_ref()
            .unwrap()
            .registry
            .slot_summary()
            .outstanding_requests
    }

    pub(crate) fn hold_serving_capacity_for_test(&self) -> Vec<Box<dyn Send>> {
        let wiring = self.routine_wiring.as_ref().unwrap();
        (0..wiring.config.get_blocks_regulation.node_active_requests)
            .map(|index| {
                let mut bytes = [0xff; 32];
                bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
                let peer = ZakuraPeerId::new(bytes.to_vec()).unwrap();
                let session = wiring.serving_regulator.session(peer);
                Box::new(session.try_admit(1).unwrap().commit()) as Box<dyn Send>
            })
            .collect()
    }
}
