use super::*;
use crate::zakura::header_sync::scheduler::repair::MAX_SUPPLIERS_PER_CYCLE;

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
fn bounded_supplier_cycles_rotate_to_the_fourth_supplier() {
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
        build_header_sync_reactor(startup).expect("the bounded round-robin repair fixture builds");
    let peers: Vec<_> = [1_u8, 2, 3, 4]
        .into_iter()
        .map(|byte| ZakuraPeerId::new(vec![byte; 32]).expect("the peer ID has the required length"))
        .collect();
    let mut _outbounds = Vec::new();
    for (index, peer) in peers.iter().enumerate() {
        let (send, outbound) = framed_channel(8);
        reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
            peer.clone(),
            7 + u64::try_from(index).expect("four peer indexes fit in u64"),
            send,
            CancellationToken::new(),
        ));
        _outbounds.push(outbound);
    }
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let context = zakura_header_chain::VctRepairContext {
        target: repair_target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    let mut task = RepairRequirement::new(owner, repair_target.height, 11);
    task.state = RepairPolicyState::Ready {
        context: context.clone(),
    };
    reactor.vct_repair.insert(task);
    let status = Status {
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
    };
    for (index, peer) in peers.iter().enumerate() {
        reactor.handle_wire_message(
            peer.clone(),
            7 + u64::try_from(index).expect("four peer indexes fit in u64"),
            HeaderSyncMessage::Status(status.clone()),
        );
    }

    for peer in &peers[..3] {
        let active = reactor
            .peer_work_queue
            .active(peer)
            .expect("the next round-robin supplier owns the repair")
            .clone();
        assert!(matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        ));
        reactor.handle_headers_outcome(
            peer.clone(),
            active.owner.session_id(),
            active.owner.header_authority(),
            HeadersOutcome {
                request_id: active.request_id.get(),
                target_tip_hash: repair_target.hash,
                outcome: HeadersOutcomeCode::TargetNotRetained,
            },
        );
    }

    assert!(peers
        .iter()
        .all(|peer| reactor.peer_work_queue.active(peer).is_none()));
    let task = reactor
        .vct_repair
        .current()
        .expect("the bounded cycle keeps the repair requirement");
    let retry_at = match &task.state {
        RepairPolicyState::SupplierBackoff {
            context: retained,
            retry_at,
        } if retained == &context => *retry_at,
        other => panic!("three supplier failures must back off, got {other:?}"),
    };
    assert_eq!(task.tried_sources.len(), 3);
    assert!(task.supplier_cycle_exhausted());
    assert_eq!(task.supplier_cursor, Some(source_id_from_peer(&peers[2])));
    let stall = reactor
        .vct_repair_stall
        .expect("a complete failed cycle starts the generation stall clock");

    reactor
        .vct_repair
        .current_mut()
        .expect("the repair remains scheduled")
        .resume_retry_cycle(retry_at);
    reactor.try_assign_vct_repair();

    let active = reactor
        .peer_work_queue
        .active(&peers[3])
        .expect("the next cycle starts after the persistent supplier cursor");
    assert!(matches!(
        active.purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
    ));
    let task = reactor
        .vct_repair
        .current()
        .expect("the fourth supplier owns the current repair");
    assert!(task.tried_sources.is_empty());
    assert_eq!(task.supplier_cursor, Some(source_id_from_peer(&peers[2])));
    assert_eq!(
        reactor
            .vct_repair_stall
            .expect("assignment preserves the generation stall clock")
            .since,
        stall.since
    );
}

#[test]
fn replacement_session_keeps_the_authenticated_supplier_identity() {
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
        build_header_sync_reactor(startup).expect("the replacement fixture builds");
    let peer = peer();
    let source = source_id_from_peer(&peer);
    let (first_send, _first_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        first_send,
        CancellationToken::new(),
    ));
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let mut task = RepairRequirement::new(owner, repair_target.height, 11);
    task.state = RepairPolicyState::Ready {
        context: zakura_header_chain::VctRepairContext {
            target: repair_target,
            locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
        },
    };
    reactor.vct_repair.insert(task);
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
    assert!(reactor.peer_work_queue.active(&peer).is_some());

    let (replacement_send, _replacement_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        8,
        replacement_send,
        CancellationToken::new(),
    ));

    assert_eq!(reactor.peer_state.len(), 1);
    assert!(reactor.peer_work_queue.active(&peer).is_none());
    let task = reactor
        .vct_repair
        .current()
        .expect("the replacement keeps the repair scheduled");
    assert_eq!(task.tried_sources, [source].into_iter().collect());
    assert_eq!(task.supplier_cursor, Some(source));
    assert!(matches!(
        task.state,
        RepairPolicyState::SupplierBackoff { .. }
    ));
}

#[test]
fn send_failures_preserve_the_stall_clock_across_bounded_cycles() {
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
        build_header_sync_reactor(startup).expect("the send-failure fixture builds");
    let peers: Vec<_> = [1_u8, 2, 3, 4]
        .into_iter()
        .map(|byte| ZakuraPeerId::new(vec![byte; 32]).expect("the peer ID is bounded"))
        .collect();
    let status = Status {
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
    };
    for (index, peer) in peers.iter().enumerate() {
        let session_id = 7 + u64::try_from(index).expect("four peer indexes fit in u64");
        let (send, outbound) = framed_channel(8);
        reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
            peer.clone(),
            session_id,
            send,
            CancellationToken::new(),
        ));
        reactor.handle_wire_message(
            peer.clone(),
            session_id,
            HeaderSyncMessage::Status(status.clone()),
        );
        drop(outbound);
    }
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let context = zakura_header_chain::VctRepairContext {
        target: repair_target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    let mut task = RepairRequirement::new(owner, repair_target.height, 11);
    task.state = RepairPolicyState::Ready {
        context: context.clone(),
    };
    reactor.vct_repair.insert(task);

    reactor.try_assign_vct_repair();

    let first_retry_at = match &reactor
        .vct_repair
        .current()
        .expect("send failures keep the repair")
        .state
    {
        RepairPolicyState::SupplierBackoff { retry_at, .. } => *retry_at,
        other => panic!("three send failures must back off, got {other:?}"),
    };
    let task = reactor.vct_repair.current().expect("the repair remains");
    assert_eq!(task.tried_sources.len(), MAX_SUPPLIERS_PER_CYCLE);
    assert_eq!(task.supplier_cursor, Some(source_id_from_peer(&peers[2])));
    let stall = reactor
        .vct_repair_stall
        .expect("send failures start the stall clock");

    reactor
        .vct_repair
        .current_mut()
        .expect("the repair remains")
        .resume_retry_cycle(first_retry_at);
    reactor.try_assign_vct_repair();

    let task = reactor.vct_repair.current().expect("the repair remains");
    assert!(matches!(
        task.state,
        RepairPolicyState::SupplierBackoff { .. }
    ));
    assert_eq!(task.tried_sources.len(), MAX_SUPPLIERS_PER_CYCLE);
    assert_eq!(task.supplier_cursor, Some(source_id_from_peer(&peers[1])));
    assert_eq!(
        reactor
            .vct_repair_stall
            .expect("the next send cycle preserves the stall clock")
            .since,
        stall.since
    );
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
