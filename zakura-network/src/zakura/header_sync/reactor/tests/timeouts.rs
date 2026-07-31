use super::*;

#[test]
fn request_timeout_retires_owned_work_and_wakes_maintenance() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the timeout fixture builds");
    let peer = peer();
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let deadline = Instant::now();
    reactor.request_deadlines.insert(peer.clone(), deadline);

    assert!(reactor.next_maintenance_deadline() <= deadline);
    reactor.retire_timed_out_requests(deadline);

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(!reactor.request_deadlines.contains_key(&peer));
    assert!(actions.try_recv().is_err());
}

#[test]
fn vct_request_timeout_keeps_required_work_and_rotates_the_supplier() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the timeout fixture builds");
    let peer = peer();
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let repair_status = &reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has one applying request")
        .target
        .status;
    let target = zakura_header_chain::Frontier::new(
        repair_status.selected_tip_height,
        repair_status.selected_tip_hash,
    );
    let mut task = RepairRequirement::new(owner, target.height, 11);
    let deadline = Instant::now();
    let context = zakura_header_chain::VctRepairContext {
        target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    task.state = RepairPolicyState::Assigned {
        context: context.clone(),
    };
    reactor.vct_repair.insert(task);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one applying request")
        .purpose = HeaderTargetPurpose::SelectedAuxiliaryRepair {
        selected_target: target,
        repair_generation: 11,
    };
    reactor.request_deadlines.insert(peer, deadline);

    assert!(reactor.next_maintenance_deadline() <= deadline);
    reactor.retire_timed_out_requests(deadline);

    let task = reactor
        .vct_repair
        .current()
        .expect("a timeout cannot discard a current repair requirement");
    assert!(matches!(
        &task.state,
        RepairPolicyState::SupplierBackoff {
            context: retained,
            retry_at,
        } if retained == &context && *retry_at > deadline
    ));
    assert_eq!(task.attempts, 1);
    assert!(task.tried_sources.contains(&source));
    assert!(task.next_deadline().is_some());
}

#[test]
fn full_action_queue_retries_lease_release_on_maintenance() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let peer = peer();
    for _ in 0..128 {
        reactor
            .actions
            .try_send(HeaderPortOperation::Misbehavior {
                peer: peer.clone(),
                reason: HeaderSyncMisbehavior::MalformedMessage,
            })
            .expect("the bounded action queue has exactly 128 slots");
    }
    let scope =
        zakura_header_chain::WorkScope::for_header_target(&snapshot, block::Hash([0x33; 32]));

    reactor.release_lease(peer.clone(), 7, 9, scope);

    assert_eq!(reactor.pending_lease_releases.len(), 1);
    assert!(reactor.lease_release_retry_at.is_some());
    let _ = actions
        .try_recv()
        .expect("draining one action creates release capacity");
    reactor.retry_pending_lease_releases(Instant::now());
    assert!(reactor.pending_lease_releases.is_empty());
    assert!(reactor.lease_release_retry_at.is_none());

    let mut found = false;
    while let Ok(action) = actions.try_recv() {
        if matches!(
            action,
            HeaderPortOperation::ReleaseHeaderPath {
                peer: actual_peer,
                session_id: 7,
                lease_id: 9,
                scope: actual_scope,
            } if actual_peer == peer && actual_scope == scope
        ) {
            found = true;
        }
    }
    assert!(
        found,
        "the retained release reaches the driver after capacity returns"
    );
}
