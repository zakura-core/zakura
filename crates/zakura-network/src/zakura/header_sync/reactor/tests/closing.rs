//! When this node closes a subscription: a `Close` the publisher never
//! answers drops the session at the request timeout, and a subscription kept
//! for the next round closes once the peer's status cannot continue it.
//!
//! Tests that call an inner handler run
//! [`HeaderSyncReactor::schedule_cached_statuses`] afterwards, as the reactor
//! loop does after every event.

use super::{
    aux_fallback::{answer_locator, connect, end, fallback_reactor, next_update, pruned_status},
    *,
};

/// A `Close` the publisher never answers drops the session at the request
/// timeout, as an unresponsive request would, so the peer cannot stay parked
/// waiting for the outcome. An answered `Close` costs nothing.
#[test]
fn an_unanswered_close_drops_the_session_at_the_request_timeout() {
    for answered in [true, false] {
        let (mut reactor, mut actions, snapshot) = fallback_reactor();
        let (peer, mut outbound) = connect(&mut reactor, 0x71);
        let target = block::Hash([0x53; 32]);
        reactor.handle_status(peer.clone(), 0, pruned_status(&snapshot, target));
        let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
        reactor.retire_peer_work(&peer, HeaderRequestTerminal::LocalError);
        assert_eq!(next_update(&mut outbound).operation, v9::Operation::Close);
        if answered {
            end(
                &mut reactor,
                &peer,
                &open,
                HeadersOutcomeCode::SubscriptionClosed,
            );
        }

        let session = reactor.peer_state[&peer].session.clone();
        reactor.retire_unanswered_closes(Instant::now());
        assert!(!session.cancel_token().is_cancelled());
        reactor.retire_unanswered_closes(Instant::now() + reactor.startup.request_timeout);
        assert_eq!(
            session.cancel_token().is_cancelled(),
            !answered,
            "only an unanswered Close drops the session"
        );
        assert_eq!(
            reactor.unproductive_peer_cooldowns.contains_key(&peer),
            !answered
        );
    }
}

/// A subscription that stopped at a committed prefix stays open for the next
/// round, and closes once the peer's status names a tip this node already
/// holds, because no round can continue it.
#[test]
fn an_idle_subscription_closes_when_no_round_can_continue_it() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    // Two headers of durable headroom make each round a two-header prefix.
    let max_nodes = u32::try_from(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        .expect("the retained-node limit fits a height");
    snapshot.frontiers.header_best = zakura_header_chain::Frontier::new(
        block::Height(anchor.height.0 + max_nodes - 2),
        block::Hash([0x51; 32]),
    );
    snapshot.frontiers.verified_best = snapshot.frontiers.header_best;
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the fixture starts");
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let mut status = pruned_status(&snapshot, target);
    status.work_anchor_height = anchor.height;
    status.selected_tip_height = block::Height(snapshot.frontiers.header_best.height.0 + 100);
    reactor.handle_status(peer.clone(), 0, status.clone());
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.added_credit.objects, 2);

    let mut parent = snapshot.frontiers.header_best;
    let mut entries = Vec::new();
    for second in 1..=2 {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(second);
        let header = Arc::new(header);
        parent = zakura_header_chain::Frontier::new(
            parent.height.next().expect("the fixture height fits"),
            header.hash(),
        );
        entries.push(HeaderEntry {
            header,
            body_size: 0,
            tree_aux: None,
        });
    }
    reactor.deliver_page(
        peer.clone(),
        0,
        Headers {
            subscription_id: open.subscription_id,
            initial_target_tip_hash: target,
            common_ancestor_height: snapshot.frontiers.header_best.height,
            common_ancestor_hash: snapshot.frontiers.header_best.hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::None,
            entries,
        },
    );
    let HeaderPortOperation::PrepareHeaderTarget { source, owner, .. } = actions
        .try_recv()
        .expect("the spent headroom prepares the prefix")
    else {
        panic!("the prefix must be prepared");
    };
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the prefix awaits admission")
        .phase = HeaderTargetPhase::Applying;
    reactor.handle_header_target_admission_ready(
        peer.clone(),
        source,
        owner,
        HeaderTargetAdmissionResult::Applied,
    );
    assert!(
        reactor.peer_state[&peer]
            .session
            .subscriber()
            .resumable()
            .is_some(),
        "the subscription is kept for the next round"
    );
    assert!(
        reactor.close_deadlines.is_empty(),
        "nothing waits on a kept subscription"
    );

    status.selected_tip_hash = snapshot.frontiers.header_best.hash;
    status.selected_tip_height = snapshot.frontiers.header_best.height;
    reactor.handle_status(peer.clone(), 0, status);
    reactor.schedule_cached_statuses();
    assert_eq!(next_update(&mut outbound).operation, v9::Operation::Close);
    assert!(reactor.close_deadlines.contains_key(&peer));
}

/// A `Close` that the session's full queue refuses starts no deadline,
/// because the publisher has nothing to answer. Its retry time stays fixed
/// while other events arrive, and the retry after the queue drains sends it
/// and starts the deadline.
#[test]
fn a_close_refused_by_a_full_queue_waits_to_start_its_deadline() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let peer = ZakuraPeerId::new(vec![0x71; 32]).expect("the peer ID has 32 bytes");
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send.clone(),
        CancellationToken::new(),
    ));
    let target = block::Hash([0x53; 32]);
    reactor.handle_status(peer.clone(), 0, pruned_status(&snapshot, target));
    answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    let filler = Frame {
        message_type: 1,
        flags: 0,
        payload: Vec::new(),
    };
    while send.try_send(filler.clone()).is_ok() {}

    reactor.retire_peer_work(&peer, HeaderRequestTerminal::LocalError);
    assert!(reactor.close_deadlines.is_empty(), "nothing was queued");
    assert!(reactor.pending_closes.contains_key(&peer));

    let (_, retry_at) = reactor.pending_closes[&peer];
    assert_eq!(reactor.next_maintenance_deadline(), retry_at);
    reactor.handle_status(peer.clone(), 0, pruned_status(&snapshot, target));
    assert_eq!(
        reactor.next_maintenance_deadline(),
        retry_at,
        "another event does not postpone the retry"
    );

    while outbound.try_recv().is_ok() {}
    reactor.retry_pending_closes(retry_at);
    assert_eq!(next_update(&mut outbound).operation, v9::Operation::Close);
    assert!(reactor.close_deadlines.contains_key(&peer));
    assert!(reactor.pending_closes.is_empty());
}
