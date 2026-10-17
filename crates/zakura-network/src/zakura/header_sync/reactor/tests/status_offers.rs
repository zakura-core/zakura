//! Each cached status starts at most one ordinary attempt.
//!
//! Tests that call an inner handler run [`HeaderSyncReactor::schedule_cached_statuses`]
//! afterwards, as the reactor loop does after every event.

use super::*;
use crate::zakura::FramedRecv;
use zakura_header_chain::MAX_STAGED_TARGETS_V1;

fn offers_reactor() -> (
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
        build_header_sync_reactor(startup).expect("the status offer fixture starts");
    (reactor, actions, snapshot)
}

fn connect(reactor: &mut HeaderSyncReactor, byte: u8) -> (ZakuraPeerId, FramedRecv) {
    let (peer, outbound, _) = connect_with_sender(reactor, byte);
    (peer, outbound)
}

/// [`connect`], also returning the session's sender, so a test can fill its
/// queue.
fn connect_with_sender(
    reactor: &mut HeaderSyncReactor,
    byte: u8,
) -> (ZakuraPeerId, FramedRecv, crate::zakura::FramedSend) {
    let peer = ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length");
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send.clone(),
        CancellationToken::new(),
    ));
    outbound.try_recv().expect("the initial status was sent");
    (peer, outbound, send)
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
        max_subscriptions: 1,
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

/// Runs many scheduler passes and asserts that none of them starts work.
fn assert_quiescent(
    reactor: &mut HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
) {
    for _ in 0..32 {
        reactor.schedule_cached_statuses();
    }
    assert!(
        actions.try_recv().is_err(),
        "no scheduler pass may restart an attempt without a fresh status"
    );
}

/// Starts `peer`'s request for `target` and returns the sent `Open`.
fn start_request(
    reactor: &mut HeaderSyncReactor,
    peer: &ZakuraPeerId,
    outbound: &mut FramedRecv,
    scope: zakura_header_chain::HeaderWorkAuthority,
    target: block::Hash,
    locator: zakura_header_chain::Frontier,
) -> v9::SubscribeHeaders {
    reactor.handle_header_locator_ready(
        peer.clone(),
        0,
        target,
        scope,
        Some(zakura_header_chain::HeaderLocator::for_continuation(
            locator,
        )),
    );
    let open = update(outbound.try_recv().expect("the Open is sent"));
    assert_eq!(open.operation, v9::Operation::Open);
    open
}

/// Starts `peer`'s request for `target`, ends it with `TargetNotRetained`, and
/// ends the event.
fn answer_unretained(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    outbound: &mut FramedRecv,
    scope: zakura_header_chain::HeaderWorkAuthority,
    target: block::Hash,
) {
    let request = start_request(
        reactor,
        peer,
        outbound,
        scope,
        target,
        snapshot.frontiers.header_best,
    );
    reactor.deliver_outcome(
        peer.clone(),
        0,
        HeadersOutcome {
            subscription_id: request.subscription_id,
            initial_target_tip_hash: target,
            outcome: HeadersOutcomeCode::TargetNotRetained,
        },
    );
    reactor.schedule_cached_statuses();
}

/// Completes `peer`'s locator read through the port-completion path and ends the event.
fn complete_locator(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    scope: zakura_header_chain::HeaderWorkAuthority,
    target: block::Hash,
    available: bool,
) {
    let peer = peer.clone();
    let locator = available.then(|| {
        zakura_header_chain::HeaderLocator::for_continuation(snapshot.frontiers.header_best)
    });
    reactor.handle_port_completion(PortOperationResult::Completed(Box::new(move |reactor| {
        reactor.handle_header_locator_ready(peer, 0, target, scope, locator);
    })));
    reactor.schedule_cached_statuses();
}

fn applying_target_status(reactor: &HeaderSyncReactor, owner: &ZakuraPeerId) -> Status {
    reactor
        .peer_work_queue
        .active(owner)
        .expect("the owner has an applying request")
        .target
        .status
        .clone()
}

#[test]
fn unretained_owner_hands_its_target_to_a_cached_alternate() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, mut owner_outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);

    reactor.handle_status(owner.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &owner, target);
    reactor.handle_status(alternate.clone(), 0, status.clone());
    assert!(
        actions.try_recv().is_err(),
        "the owned target holds back the alternate's status"
    );

    answer_unretained(
        &mut reactor,
        &snapshot,
        &owner,
        &mut owner_outbound,
        scope,
        target,
    );
    expect_locator_query(&mut actions, &alternate, target);
    reactor.handle_status(owner.clone(), 0, status);
    assert!(
        actions.try_recv().is_err(),
        "the failed owner's next status cannot take the target back"
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
fn status_sent_during_the_owners_attempt_cannot_reclaim_its_target() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, mut owner_outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);

    reactor.handle_status(owner.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &owner, target);
    reactor.handle_status(alternate.clone(), 0, status.clone());
    let request = start_request(
        &mut reactor,
        &owner,
        &mut owner_outbound,
        scope,
        target,
        snapshot.frontiers.header_best,
    );
    // The owner re-advertises before its outcome arrives.
    reactor.handle_status(owner.clone(), 0, status);
    reactor.deliver_outcome(
        owner.clone(),
        0,
        HeadersOutcome {
            subscription_id: request.subscription_id,
            initial_target_tip_hash: target,
            outcome: HeadersOutcomeCode::TargetNotRetained,
        },
    );
    reactor.schedule_cached_statuses();

    expect_locator_query(&mut actions, &alternate, target);
    assert!(reactor.peer_work_queue.awaiting_target(&owner).is_none());
}

#[test]
fn peers_that_both_fail_a_target_wait_for_a_fresh_status() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, mut first_outbound) = connect(&mut reactor, 0x71);
    let (second, mut second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);

    reactor.handle_status(first.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &first, target);
    reactor.handle_status(second.clone(), 0, status);
    answer_unretained(
        &mut reactor,
        &snapshot,
        &first,
        &mut first_outbound,
        scope,
        target,
    );
    let scope = expect_locator_query(&mut actions, &second, target);
    answer_unretained(
        &mut reactor,
        &snapshot,
        &second,
        &mut second_outbound,
        scope,
        target,
    );

    assert_quiescent(&mut reactor, &mut actions);
    assert!(reactor.peer_work_queue.awaiting_target(&first).is_none());
    assert!(reactor.peer_work_queue.awaiting_target(&second).is_none());
}

#[test]
fn fresh_status_restores_the_peers_attempt() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, mut first_outbound) = connect(&mut reactor, 0x71);
    let (second, mut second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_status(first.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &first, target);
    reactor.handle_status(second.clone(), 0, status.clone());
    answer_unretained(
        &mut reactor,
        &snapshot,
        &first,
        &mut first_outbound,
        scope,
        target,
    );
    let scope = expect_locator_query(&mut actions, &second, target);

    reactor.handle_status(first.clone(), 0, status);
    assert!(
        actions.try_recv().is_err(),
        "the second peer still owns the target"
    );
    answer_unretained(
        &mut reactor,
        &snapshot,
        &second,
        &mut second_outbound,
        scope,
        target,
    );
    expect_locator_query(&mut actions, &first, target);
}

#[test]
fn header_authority_change_restores_spent_statuses() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, _first_outbound) = connect(&mut reactor, 0x71);
    let (second, _second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_status(first.clone(), 0, status.clone());
    expect_locator_query(&mut actions, &first, target);
    reactor.handle_status(second.clone(), 0, status);
    reactor.retire_peer_work(&first, HeaderRequestTerminal::LocalError);
    reactor.schedule_cached_statuses();
    expect_locator_query(&mut actions, &second, target);
    reactor.retire_peer_work(&second, HeaderRequestTerminal::LocalError);
    assert_quiescent(&mut reactor, &mut actions);

    let mut newer = snapshot;
    newer.header_generation = newer.header_generation.checked_next().unwrap();
    newer.state_version = newer.state_version.checked_next().unwrap();
    reactor.observe_latest_committed_snapshot(newer);
    expect_locator_query(&mut actions, &first, target);
    assert!(
        actions.try_recv().is_err(),
        "the restored statuses still name one target, so only one peer pursues it"
    );
}

#[test]
fn status_held_back_at_capacity_is_staged_when_a_slot_frees() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
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
    let (held_back, _held_back_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);

    reactor.handle_status(held_back.clone(), 0, status_for(&snapshot, target));
    assert!(
        actions.try_recv().is_err(),
        "the full queue holds back the status"
    );

    reactor.retire_peer_work(&fillers[0], HeaderRequestTerminal::LocalError);
    reactor.schedule_cached_statuses();
    expect_locator_query(&mut actions, &held_back, target);
}

#[test]
fn completion_release_stages_a_cached_alternate() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = applying_target_status(&reactor, &owner);
    let target = status.selected_tip_hash;
    reactor.handle_status(alternate.clone(), 0, status);
    assert!(actions.try_recv().is_err());

    reactor.handle_port_completion(PortOperationResult::Completed(Box::new(move |reactor| {
        reactor.retire_peer_work(&owner, HeaderRequestTerminal::LocalError);
    })));
    reactor.schedule_cached_statuses();
    expect_locator_query(&mut actions, &alternate, target);
}

#[test]
fn applied_target_is_not_staged_for_alternates() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let (source, owner_scope, _) = seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = applying_target_status(&reactor, &owner);
    reactor.handle_status(alternate, 0, status);
    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: owner,
        source,
        owner: owner_scope,
        result: HeaderTargetAdmissionResult::Applied,
    });
    assert_quiescent(&mut reactor, &mut actions);
}

#[test]
fn already_known_target_is_not_staged_for_alternates() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, mut owner_outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_status(owner.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &owner, target);
    reactor.handle_status(alternate, 0, status);

    // The locator already contains the target, so no subscription opens.
    let known = zakura_header_chain::Frontier::new(block::Height(2), target);
    reactor.handle_header_locator_ready(
        owner.clone(),
        0,
        target,
        scope,
        Some(zakura_header_chain::HeaderLocator::for_continuation(known)),
    );
    assert!(owner_outbound.try_recv().is_err());
    assert!(reactor.peer_work_queue.active(&owner).is_none());
    assert_quiescent(&mut reactor, &mut actions);
}

#[test]
fn resource_stall_waits_before_staging_an_alternate() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    let (alternate, _alternate_outbound) = connect(&mut reactor, 0x72);
    let (source, owner_scope, branch) =
        seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let status = applying_target_status(&reactor, &owner);
    reactor.handle_status(alternate.clone(), 0, status.clone());
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
    reactor.handle_status(alternate.clone(), 0, status.clone());
    assert_quiescent(&mut reactor, &mut actions);

    let mut newer = snapshot;
    newer.state_version = newer.state_version.checked_next().unwrap();
    reactor.observe_latest_committed_snapshot(newer);
    expect_locator_query(&mut actions, &alternate, status.selected_tip_hash);
}

#[test]
fn status_too_small_for_one_header_is_not_staged() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, _first_outbound) = connect(&mut reactor, 0x71);
    let (second, _second_outbound) = connect(&mut reactor, 0x72);
    let mut status = status_for(&snapshot, block::Hash([0x42; 32]));
    status.max_message_bytes = 1;

    reactor.handle_status(first.clone(), 0, status.clone());
    reactor.handle_status(second.clone(), 0, status);
    assert_quiescent(&mut reactor, &mut actions);
    assert!(reactor.peer_work_queue.awaiting_target(&first).is_none());
    assert!(reactor.peer_work_queue.awaiting_target(&second).is_none());
}

#[test]
fn failed_locator_reads_do_not_alternate_between_cached_statuses() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, _first_outbound) = connect(&mut reactor, 0x71);
    let (second, _second_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_status(first.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &first, target);
    reactor.handle_status(second.clone(), 0, status);

    complete_locator(&mut reactor, &snapshot, &first, scope, target, false);
    let scope = expect_locator_query(&mut actions, &second, target);
    complete_locator(&mut reactor, &snapshot, &second, scope, target, false);
    assert_quiescent(&mut reactor, &mut actions);
}

#[test]
fn failed_sends_do_not_alternate_between_cached_statuses() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (first, first_outbound) = connect(&mut reactor, 0x71);
    let (second, second_outbound) = connect(&mut reactor, 0x72);
    drop(first_outbound);
    drop(second_outbound);
    let target = block::Hash([0x42; 32]);
    let status = status_for(&snapshot, target);
    reactor.handle_status(first.clone(), 0, status.clone());
    let scope = expect_locator_query(&mut actions, &first, target);
    reactor.handle_status(second.clone(), 0, status);

    complete_locator(&mut reactor, &snapshot, &first, scope, target, true);
    assert!(reactor.peer_work_queue.active(&first).is_none());
    let scope = expect_locator_query(&mut actions, &second, target);
    complete_locator(&mut reactor, &snapshot, &second, scope, target, true);
    assert!(reactor.peer_work_queue.active(&second).is_none());
    assert_quiescent(&mut reactor, &mut actions);
}

#[test]
fn headroom_counts_pending_locator_reads_at_their_negotiated_size() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, _outbound) = connect(&mut reactor, 0x71);
    seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    // Two headers of shared headroom remain.
    reactor
        .peer_work_queue
        .set_capacity_for_test(&owner, 3998, 0);
    let one_header_bytes = (1..=4096)
        .find(|bytes| {
            v9::page_capacity(
                header_sync_header_bytes_for_network(&reactor.startup.network),
                AuxSchema::None,
                *bytes,
            ) == 1
        })
        .expect("some message size fits exactly one header");

    // The first peer advertises many headers per response but bytes for only one.
    let (byte_limited, _byte_limited_outbound) = connect(&mut reactor, 0x72);
    let mut byte_limited_status = status_for(&snapshot, block::Hash([0x72; 32]));
    byte_limited_status.max_headers_per_response = 250;
    byte_limited_status.max_message_bytes = u32::try_from(one_header_bytes).unwrap();
    reactor.handle_status(byte_limited.clone(), 0, byte_limited_status);
    expect_locator_query(&mut actions, &byte_limited, block::Hash([0x72; 32]));

    let (second, _second_outbound) = connect(&mut reactor, 0x73);
    reactor.handle_status(
        second.clone(),
        0,
        status_for(&snapshot, block::Hash([0x73; 32])),
    );
    expect_locator_query(&mut actions, &second, block::Hash([0x73; 32]));

    let (third, _third_outbound) = connect(&mut reactor, 0x74);
    reactor.handle_status(
        third.clone(),
        0,
        status_for(&snapshot, block::Hash([0x74; 32])),
    );
    assert!(
        actions.try_recv().is_err(),
        "two pending one-header reads use all remaining headroom"
    );
    assert!(!reactor.peer_state[&third].status_spent);
}

#[test]
fn locator_losing_headroom_keeps_offer_for_body_progress() {
    let (mut reactor, mut actions, mut snapshot) = offers_reactor();
    reactor.startup.committed_snapshots = None;
    snapshot.frontiers.header_best = zakura_header_chain::Frontier::new(
        block::Height(INTEGRATED_HEADER_BODY_WINDOW_V1 - 401),
        block::Hash([0x40; 32]),
    );
    reactor.observe_latest_committed_snapshot(snapshot.clone());
    let (owner, _owner_outbound) = connect(&mut reactor, 0x71);
    seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let (waiting, mut outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    let mut status = status_for(&snapshot, target);
    status.selected_tip_height = block::Height(INTEGRATED_HEADER_BODY_WINDOW_V1 + 1);
    reactor.handle_status(waiting.clone(), 0, status);
    let scope = expect_locator_query(&mut actions, &waiting, target);

    // A continuation can enlarge its claim while the locator read is in flight.
    reactor
        .peer_work_queue
        .set_capacity_for_test(&owner, 151, 250);
    complete_locator(&mut reactor, &snapshot, &waiting, scope, target, true);
    assert!(outbound.try_recv().is_err());
    assert_quiescent(&mut reactor, &mut actions);
    assert!(!reactor.peer_state[&waiting].status_spent);

    // Only verified bodies advance: neither header authority nor the peer status changes.
    snapshot.state_version = snapshot.state_version.checked_next().unwrap();
    snapshot.frontiers.verified_best =
        zakura_header_chain::Frontier::new(block::Height(250), block::Hash([0x43; 32]));
    reactor.observe_latest_committed_snapshot(snapshot.clone());
    let scope = expect_locator_query(&mut actions, &waiting, target);
    start_request(
        &mut reactor,
        &waiting,
        &mut outbound,
        scope,
        target,
        snapshot.frontiers.header_best,
    );
}

#[test]
fn in_flight_locator_waits_for_committed_resource_stall_to_clear() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (owner, _owner_outbound) = connect(&mut reactor, 0x71);
    let (source, owner_scope, branch) =
        seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
    let (waiting, mut outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x42; 32]);
    reactor.handle_status(waiting.clone(), 0, status_for(&snapshot, target));
    let scope = expect_locator_query(&mut actions, &waiting, target);
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
    complete_locator(&mut reactor, &snapshot, &waiting, scope, target, true);
    assert!(
        outbound.try_recv().is_err(),
        "a ready locator cannot bypass the committed stall"
    );
    assert_eq!(reactor.peer_work_queue.reserved_header_count(&waiting), 0);
    assert_quiescent(&mut reactor, &mut actions);
    assert!(!reactor.peer_state[&waiting].status_spent);
    let mut newer = snapshot;
    newer.state_version = newer.state_version.checked_next().unwrap();
    reactor.observe_latest_committed_snapshot(newer.clone());
    let scope = expect_locator_query(&mut actions, &waiting, target);
    start_request(
        &mut reactor,
        &waiting,
        &mut outbound,
        scope,
        target,
        newer.frontiers.header_best,
    );
}

fn ready_repair(
    reactor: &mut HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    snapshot: &zakura_header_chain::EngineSnapshot,
) {
    reactor.observe_vct_root_repair(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: block::Height(1),
        },
        generation: 7,
    });
    let HeaderPortOperation::QueryVctRepairContext { owner, .. } = actions.try_recv().unwrap()
    else {
        panic!("repair must request context");
    };
    let context = zakura_header_chain::VctRepairContext::from_durable_rows(
        snapshot.frontiers.header_best,
        zakura_header_chain::HeaderLocator::for_continuation(snapshot.frontiers.finalized),
        snapshot.state_version,
        None,
        true,
        &[],
    )
    .unwrap();
    reactor.handle_vct_repair_context_ready(owner, VctRepairContextResult::Resolved(context));
}

#[test]
fn repair_send_failure_preserves_ordinary_offer() {
    assert_failed_repair_preserves_offer(false);
}

#[test]
fn repair_budget_failure_preserves_ordinary_offer() {
    assert_failed_repair_preserves_offer(true);
}

fn assert_failed_repair_preserves_offer(full_budget: bool) {
    let (mut reactor, mut actions, mut snapshot) = offers_reactor();
    reactor.startup.committed_snapshots = None;
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x40; 32]));
    reactor.observe_latest_committed_snapshot(snapshot.clone());
    let (supplier, mut outbound, send) = connect_with_sender(&mut reactor, 0x71);
    let target = block::Hash([0x42; 32]);
    let mut status = status_for(&snapshot, target);
    status.tree_aux_schema_mask = AuxSchema::V1.mask_bit();
    reactor.handle_status(supplier.clone(), 0, status);
    expect_locator_query(&mut actions, &supplier, target);
    let (owner, _owner_outbound) = connect(&mut reactor, 0x72);
    if full_budget {
        seed_applying_request(&mut reactor, &snapshot, owner.clone(), 0);
        reactor
            .peer_work_queue
            .set_capacity_for_test(&owner, 4000, 0);
    } else {
        // Fill the ordered stream so the repair send fails without disconnecting it.
        while send
            .try_send(Frame {
                message_type: 1,
                flags: 0,
                payload: Vec::new(),
            })
            .is_ok()
        {}
    }
    ready_repair(&mut reactor, &mut actions, &snapshot);
    assert!(reactor.peer_work_queue.active(&supplier).is_none());
    assert!(
        !reactor.peer_state[&supplier].status_spent,
        "failed repair must preserve the ordinary offer (full_budget={full_budget})"
    );
    if full_budget {
        reactor.peer_work_queue.remove(&owner);
    } else {
        while outbound.try_recv().is_ok() {}
    }
    reactor.schedule_cached_statuses();
    let scope = expect_locator_query(&mut actions, &supplier, target);
    start_request(
        &mut reactor,
        &supplier,
        &mut outbound,
        scope,
        target,
        snapshot.frontiers.header_best,
    );
}

#[test]
fn status_during_repair_keeps_its_ordinary_attempt() {
    let (mut reactor, mut actions, mut snapshot) = offers_reactor();
    reactor.startup.committed_snapshots = None;
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x40; 32]));
    reactor.observe_latest_committed_snapshot(snapshot.clone());
    let (supplier, mut outbound) = connect(&mut reactor, 0x71);
    ready_repair(&mut reactor, &mut actions, &snapshot);
    let target = block::Hash([0x42; 32]);
    let mut status = status_for(&snapshot, target);
    status.tree_aux_schema_mask = AuxSchema::V1.mask_bit();
    reactor.handle_status(supplier.clone(), 0, status.clone());
    assert!(matches!(
        reactor.peer_work_queue.active(&supplier).unwrap().purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
    ));
    let repair = update(outbound.try_recv().expect("repair Open sent"));
    assert!(
        !reactor.peer_state[&supplier].status_spent,
        "the status that assigns repair still offers ordinary work"
    );
    reactor.handle_status(supplier.clone(), 0, status);
    assert!(
        !reactor.peer_state[&supplier].status_spent,
        "a fresh status during repair is not an ordinary attempt"
    );
    reactor.retire_vct_repair();
    assert_eq!(
        update(outbound.try_recv().expect("repair Close sent")).operation,
        v9::Operation::Close
    );
    reactor.schedule_cached_statuses();
    assert!(
        actions.try_recv().is_err(),
        "the next Open waits for the repair's terminal outcome"
    );
    reactor.deliver_outcome(
        supplier.clone(),
        0,
        HeadersOutcome {
            subscription_id: repair.subscription_id,
            initial_target_tip_hash: repair.initial_target_tip_hash,
            outcome: HeadersOutcomeCode::SubscriptionClosed,
        },
    );
    reactor.schedule_cached_statuses();
    let scope = expect_locator_query(&mut actions, &supplier, target);
    start_request(
        &mut reactor,
        &supplier,
        &mut outbound,
        scope,
        target,
        snapshot.frontiers.header_best,
    );
}

/// A full cadence burst of repeated statuses from each of `peers` peers
/// starts one locator read, and another peer's new target still starts at
/// once. The port never answers, so each read stays pending, as under a slow
/// state.
#[allow(clippy::print_stderr)]
fn status_flood(peers: u8) {
    // The `Status` cadence capacity.
    const BURST: usize = 602;
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = v9::FakePort::new(0);
    startup.use_direct_port();
    let (_, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the reactor builds");
    let flooders: Vec<_> = (0..peers).map(|byte| connect(&mut reactor, byte)).collect();
    let (honest, _outbound) = connect(&mut reactor, 0xff);
    let flooded = block::Hash([0x42; 32]);
    let started = std::time::Instant::now();
    for _ in 0..BURST {
        for (peer, _) in &flooders {
            reactor.handle_status(peer.clone(), 0, status_for(&snapshot, flooded));
            reactor.schedule_cached_statuses();
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(reactor.pending_port_operations.len(), 1);
    reactor.handle_status(honest, 0, status_for(&snapshot, block::Hash([0x43; 32])));
    assert_eq!(reactor.pending_port_operations.len(), 2);
    let statuses = BURST * flooders.len();
    eprintln!(
        "status_flood peers={peers} statuses={statuses} elapsed={elapsed:?} ns_per_status={}",
        elapsed.as_nanos() / u128::try_from(statuses).expect("fits u128"),
    );
}

/// Sixteen flooding peers, on every pull request.
#[test]
fn status_floods_start_no_repeated_work() {
    status_flood(16);
}

/// Every peer a one-byte test identity allows but the honest one, in
/// full-coverage CI runs.
#[test]
#[ignore = "broad load run; full-coverage CI passes --run-ignored"]
fn maximum_status_floods_start_no_repeated_work() {
    status_flood(255);
}

/// A status for another tip that arrives while a target applies stays
/// unspent. If the reactor stages it before it sees the applied snapshot, the
/// snapshot retires that attempt and stages the tip again under the new header
/// authority, with no further status from the peer.
#[test]
fn a_new_tip_during_application_starts_under_the_applied_authority() {
    let (mut reactor, mut actions, snapshot) = offers_reactor();
    let (peer, _outbound) = connect(&mut reactor, 0x71);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 0);
    let next_tip = block::Hash([0x43; 32]);
    reactor.handle_status(peer.clone(), 0, status_for(&snapshot, next_tip));
    reactor.schedule_cached_statuses();
    assert!(
        actions.try_recv().is_err(),
        "the applying target holds the peer"
    );

    // The target applies, and the reactor schedules before the new snapshot.
    reactor.handle_header_target_admission_ready(
        peer.clone(),
        source,
        owner,
        HeaderTargetAdmissionResult::Applied,
    );
    reactor.schedule_cached_statuses();
    let stale = expect_locator_query(&mut actions, &peer, next_tip);

    let mut applied = snapshot;
    applied.state_version = applied.state_version.checked_next().unwrap();
    applied.header_generation = applied.header_generation.checked_next().unwrap();
    reactor.observe_latest_committed_snapshot(applied.clone());
    let current = expect_locator_query(&mut actions, &peer, next_tip);
    assert_ne!(current, stale);
    assert_eq!(current.header_generation, applied.header_generation);
}
