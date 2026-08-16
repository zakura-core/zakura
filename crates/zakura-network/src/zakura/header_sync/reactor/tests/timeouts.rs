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
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot)
        .bind(owner.session_id(), owner.request_id());
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one applying request")
        .owner = owner.into();
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
fn initial_vct_wire_assignment_arms_the_request_deadline() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    let repair_target =
        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x41; 32]));
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0x42; 32]));
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the repair timeout fixture builds");
    let peer = peer();
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let mut repair = RepairRequirement::new(owner, repair_target.height, 11);
    repair.state = RepairPolicyState::Ready {
        context: zakura_header_chain::VctRepairContext {
            target: repair_target,
            locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
        },
    };
    reactor.vct_repair.insert(repair);
    let before = Instant::now();

    reactor.handle_wire_message(
        peer.clone(),
        7,
        HeaderSyncMessage::Status(Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
        }),
    );

    assert!(matches!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| &active.purpose),
        Some(HeaderTargetPurpose::SelectedAuxiliaryRepair { .. })
    ));
    let deadline = reactor
        .request_deadlines
        .get(&peer)
        .copied()
        .expect("the exact repair wire request owns a deadline");
    assert!(deadline >= before + reactor.startup.request_timeout);
}

#[test]
fn bounded_supplier_cycle_backs_off_while_fresh_peers_remain() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    let repair_target =
        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x41; 32]));
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0x42; 32]));
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the bounded repair fixture builds");
    let peer = peer();
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let context = zakura_header_chain::VctRepairContext {
        target: repair_target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    let mut repair = RepairRequirement::new(owner, repair_target.height, 11);
    repair.state = RepairPolicyState::Ready {
        context: context.clone(),
    };
    for byte in [0x11_u8, 0x22, 0x33] {
        repair
            .tried_sources
            .insert(zakura_header_chain::SourceId::from_digest([byte; 32]));
    }
    assert!(repair.supplier_cycle_exhausted());
    reactor.vct_repair.insert(repair);

    reactor.handle_wire_message(
        peer.clone(),
        7,
        HeaderSyncMessage::Status(Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
        }),
    );

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    let task = reactor
        .vct_repair
        .current()
        .expect("the bounded cycle keeps the repair requirement");
    assert!(matches!(
        &task.state,
        RepairPolicyState::SupplierBackoff {
            context: retained,
            ..
        } if retained == &context
    ));
    assert_eq!(task.tried_sources.len(), 3);
    assert!(task.supplier_cycle_exhausted());
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
        zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, block::Hash([0x33; 32]));

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
