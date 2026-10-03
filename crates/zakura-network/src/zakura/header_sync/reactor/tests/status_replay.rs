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
