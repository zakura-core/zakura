use super::*;
use crate::zakura::FramedRecv;
use zakura_header_chain::MAX_STAGED_TARGETS_V1;

fn replay_reactor() -> (
    HeaderSyncReactor,
    mpsc::Receiver<HeaderPortOperation>,
    zakura_header_chain::EngineSnapshot,
) {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_, actions, reactor) =
        build_header_sync_reactor(startup).expect("the replay fixture starts");
    (reactor, actions, snapshot)
}

fn connect(reactor: &mut HeaderSyncReactor, byte: u8) -> (ZakuraPeerId, FramedRecv) {
    let peer = ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length");
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send,
        CancellationToken::new(),
    ));
    outbound.try_recv().expect("the initial status was sent");
    (peer, outbound)
}

fn status_for(snapshot: &zakura_header_chain::EngineSnapshot, target: block::Hash) -> Status {
    let anchor = snapshot.frontiers.finalized;
    Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: block::Height(2),
        selected_tip_hash: target,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    }
}

fn expect_locator_query(
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    expected_peer: &ZakuraPeerId,
    target: block::Hash,
) -> zakura_header_chain::HeaderWorkAuthority {
    match actions.try_recv() {
        Ok(HeaderPortOperation::QueryHeaderLocator {
            peer,
            target_tip_hash,
            scope,
            ..
        }) if &peer == expected_peer && target_tip_hash == target => scope,
        other => panic!("expected a locator query for {expected_peer:?}, got {other:?}"),
    }
}

/// Starts `peer`'s request for `target` and answers it with Busy.
fn answer_busy(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    outbound: &mut FramedRecv,
    scope: zakura_header_chain::HeaderWorkAuthority,
    target: block::Hash,
) {
    reactor.handle_header_locator_ready(
        peer.clone(),
        0,
        target,
        scope,
        Some(zakura_header_chain::HeaderLocator::for_continuation(
            snapshot.frontiers.header_best,
        )),
    );
    let HeaderSyncMessage::GetHeaders(request) = reactor
        .codec
        .decode_frame(outbound.try_recv().expect("the request is sent"), None)
        .expect("the request decodes")
    else {
        panic!("the locator must start a GetHeaders request");
    };
    reactor.handle_headers_outcome(
        peer.clone(),
        0,
        scope,
        HeadersOutcome {
            request_id: request.request_id,
            target_tip_hash: target,
            outcome: HeadersOutcomeCode::Busy,
        },
    );
}

#[test]
fn busy_owner_hands_its_target_to_a_retained_alternate() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (owner, mut owner_outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);

    reactor.handle_wire_message(owner.clone(), 0, HeaderSyncMessage::Status(status.clone()));
    let scope = expect_locator_query(&mut actions, &owner, target);
    reactor.handle_wire_message(
        alternate.clone(),
        0,
        HeaderSyncMessage::Status(status.clone()),
    );
    assert!(
        actions.try_recv().is_err(),
        "the owned target suppresses the alternate's status"
    );

    answer_busy(
        &mut reactor,
        &snapshot,
        &owner,
        &mut owner_outbound,
        scope,
        target,
    );
    expect_locator_query(&mut actions, &alternate, target);
    reactor.handle_wire_message(owner.clone(), 0, HeaderSyncMessage::Status(status));
    assert!(
        actions.try_recv().is_err(),
        "the Busy owner's immediate status cannot take the target back"
    );
    assert!(reactor.peer_work_queue.awaiting_target(&owner).is_none());
    assert_eq!(
        reactor
            .peer_work_queue
            .awaiting_target(&alternate)
            .map(|target| target.status.selected_tip_hash),
        Some(target)
    );
}

#[test]
fn replay_skips_a_peer_for_the_target_it_failed() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (first, mut first_outbound) = connect(&mut reactor, 0x71);
    let (second, mut second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);

    reactor.handle_wire_message(first.clone(), 0, HeaderSyncMessage::Status(status.clone()));
    let scope = expect_locator_query(&mut actions, &first, target);
    reactor.handle_wire_message(second.clone(), 0, HeaderSyncMessage::Status(status));
    answer_busy(
        &mut reactor,
        &snapshot,
        &first,
        &mut first_outbound,
        scope,
        target,
    );
    let scope = expect_locator_query(&mut actions, &second, target);

    answer_busy(
        &mut reactor,
        &snapshot,
        &second,
        &mut second_outbound,
        scope,
        target,
    );
    assert!(
        actions.try_recv().is_err(),
        "the first peer already failed this target, and neither peer is scored or dropped"
    );
    assert!(reactor.peer_work_queue.awaiting_target(&first).is_none());
    assert!(reactor.peer_work_queue.awaiting_target(&second).is_none());
}

#[test]
fn status_refused_at_capacity_is_replayed_when_a_slot_frees() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let fillers: Vec<_> = (0..MAX_STAGED_TARGETS_V1)
        .map(|index| {
            let byte = u8::try_from(index).expect("the staged target cap fits u8");
            let peer = ZakuraPeerId::new(vec![byte; 32])
                .expect("the test peer ID has the required length");
            let target = block::Hash([byte; 32]);
            let staged = AdvertisedHeaderTarget {
                scope: zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target),
                session_id: 0,
                status: status_for(&snapshot, target),
            };
            assert_eq!(
                reactor.peer_work_queue.stage(
                    peer.clone(),
                    staged,
                    PeerWorkPriority::HigherComparableWork
                ),
                QueueWorkResult::NeedsLocator
            );
            peer
        })
        .collect();
    let (refused, _refused_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);

    reactor.handle_wire_message(
        refused.clone(),
        0,
        HeaderSyncMessage::Status(status_for(&snapshot, target)),
    );
    assert!(
        actions.try_recv().is_err(),
        "the full queue refuses the status"
    );

    reactor.retire_peer_work(&fillers[0], HeaderRequestTerminal::Busy);
    expect_locator_query(&mut actions, &refused, target);
}

#[test]
fn applied_target_is_suppressed_before_replaying_alternates() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let (source, owner_scope, _) = seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = reactor
        .peer_work_queue
        .active(&owner)
        .unwrap()
        .target
        .status
        .clone();
    reactor.handle_wire_message(alternate, 0, HeaderSyncMessage::Status(status));
    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: owner,
        source,
        owner: owner_scope,
        result: HeaderTargetAdmissionResult::Applied,
    });
    assert!(
        actions.try_recv().is_err(),
        "a completed target must never dispatch an alternate locator"
    );
}

#[test]
fn resource_stall_waits_before_replaying_an_alternate() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let (source, owner_scope, branch) =
        seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = reactor
        .peer_work_queue
        .active(&owner)
        .unwrap()
        .target
        .status
        .clone();
    reactor.handle_wire_message(
        alternate.clone(),
        0,
        HeaderSyncMessage::Status(status.clone()),
    );
    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: owner,
        source,
        owner: owner_scope,
        result: HeaderTargetAdmissionResult::ResourceStalled(
            zakura_header_chain::CommittedStallReceipt {
                state_version: snapshot.state_version,
                alarm_changed: true,
                attempted_branch: Some(branch),
            },
        ),
    });
    reactor.handle_wire_message(
        alternate.clone(),
        0,
        HeaderSyncMessage::Status(status.clone()),
    );
    assert!(
        actions.try_recv().is_err(),
        "an unchanged local resource stall must not cascade to another supplier"
    );
    let mut newer = snapshot;
    newer.state_version = newer.state_version.checked_next().unwrap();
    reactor.observe_latest_committed_snapshot(newer);
    expect_locator_query(&mut actions, &alternate, status.selected_tip_hash);
}

#[test]
fn local_retirement_does_not_permanently_suppress_cached_status() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (first, _first_outbound) = connect(&mut reactor, 0x71);
    let (second, _second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_wire_message(first.clone(), 0, HeaderSyncMessage::Status(status.clone()));
    expect_locator_query(&mut actions, &first, target);
    reactor.handle_wire_message(second.clone(), 0, HeaderSyncMessage::Status(status));
    reactor.retire_peer_work(&first, HeaderRequestTerminal::LocalError);
    expect_locator_query(&mut actions, &second, target);
    reactor.retire_peer_work(&second, HeaderRequestTerminal::Busy);
    expect_locator_query(&mut actions, &first, target);
}

#[test]
fn direct_completion_release_replays_cached_alternate() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = reactor
        .peer_work_queue
        .active(&owner)
        .unwrap()
        .target
        .status
        .clone();
    let target = status.selected_tip_hash;
    reactor.handle_wire_message(alternate.clone(), 0, HeaderSyncMessage::Status(status));
    reactor.handle_port_completion(PortOperationResult::Completed(Box::new(move |reactor| {
        reactor.retire_peer_work_without_replay(&owner, HeaderRequestTerminal::LocalError);
    })));
    expect_locator_query(&mut actions, &alternate, target);
}

#[test]
fn replay_accounts_for_pending_locator_headroom() {
    let (mut reactor, mut actions, snapshot) = replay_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    reactor
        .peer_work_queue
        .set_capacity_for_test(&owner, 3999, 0);
    let mut outbound = Vec::new();
    for byte in [0x72, 0x73] {
        let (peer, receiver) = connect(&mut reactor, byte);
        outbound.push(receiver);
        reactor.peer_state.get_mut(&peer).unwrap().last_status =
            Some(status_for(&snapshot, block::Hash([byte; 32])));
    }
    reactor.replay_retained_statuses(&HashSet::from([owner]));
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::QueryHeaderLocator { .. })
    ));
    assert!(
        actions.try_recv().is_err(),
        "one remaining header slot must not launch two locator reads"
    );
}
