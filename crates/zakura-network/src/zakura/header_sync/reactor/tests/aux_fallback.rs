//! An ordinary round that ends `HistoryPruned` changes how the reactor uses
//! that peer, without penalty: a schema 1 round falls back to rounds without
//! records and probes schema 1 again after a bounded backoff, and a round
//! without records pauses the peer so another one takes the target.
//!
//! Tests that call an inner handler run
//! [`HeaderSyncReactor::schedule_cached_statuses`] afterwards, as the reactor
//! loop does after every event.

use super::*;
use crate::zakura::FramedRecv;

/// A reactor at the Regtest genesis.
fn fallback_reactor() -> (
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
        build_header_sync_reactor(startup).expect("the fallback fixture starts");
    (reactor, actions, snapshot)
}

/// Connect the peer `byte` and drop its initial status.
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

/// A status for `target` whose work anchor covers every round, so only the
/// fallback can drop schema 1.
fn pruned_status(snapshot: &zakura_header_chain::EngineSnapshot, target: block::Hash) -> Status {
    Status {
        work_anchor_height: block::Height(5_000),
        work_anchor_hash: snapshot.frontiers.finalized.hash,
        selected_tip_height: block::Height(10_000),
        selected_tip_hash: target,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: snapshot.frontiers.finalized.height,
        max_headers_per_response: 1_000,
        max_subscriptions: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
    }
}

/// Deliver `status` from `peer` and run the scheduler.
fn refresh(reactor: &mut HeaderSyncReactor, peer: &ZakuraPeerId, status: &Status) {
    reactor.handle_status(peer.clone(), 0, status.clone());
    reactor.schedule_cached_statuses();
}

/// Answer `peer`'s locator query for `target` with the validated tip, and
/// return the subscription update the round sends.
fn answer_locator(
    reactor: &mut HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    peer: &ZakuraPeerId,
    outbound: &mut FramedRecv,
    target: block::Hash,
) -> v9::SubscribeHeaders {
    let scope = match actions.try_recv() {
        Ok(HeaderPortOperation::QueryHeaderLocator {
            peer: queried,
            target_tip_hash,
            scope,
            ..
        }) if &queried == peer && target_tip_hash == target => scope,
        other => panic!("expected a locator query for {peer:?}, got {other:?}"),
    };
    let tip = reactor
        .committed_snapshot
        .as_ref()
        .expect("the fixture has a committed snapshot")
        .frontiers
        .header_best;
    reactor.handle_header_locator_ready(
        peer.clone(),
        0,
        target,
        scope,
        Some(zakura_header_chain::HeaderLocator::for_continuation(tip)),
    );
    next_update(outbound)
}

/// The next subscription update on `outbound`, skipping `Status` frames.
fn next_update(outbound: &mut FramedRecv) -> v9::SubscribeHeaders {
    loop {
        let frame = outbound.try_recv().expect("the round sent an update");
        if let Ok(v9::Message::SubscribeHeaders(update)) =
            decode_frame::<v9::RegtestMessage>(&frame)
        {
            return update;
        }
    }
}

/// End `open`'s subscription with `outcome` and run the scheduler.
fn end(
    reactor: &mut HeaderSyncReactor,
    peer: &ZakuraPeerId,
    open: &v9::SubscribeHeaders,
    outcome: HeadersOutcomeCode,
) {
    reactor.deliver_outcome(
        peer.clone(),
        0,
        HeadersOutcome {
            subscription_id: open.subscription_id,
            initial_target_tip_hash: open.initial_target_tip_hash,
            outcome,
        },
    );
    reactor.schedule_cached_statuses();
}

/// Assert that `peer` stays connected with no strike or penalty action.
fn assert_unpunished(
    reactor: &HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    peer: &ZakuraPeerId,
) {
    let state = reactor
        .peer_state
        .get(peer)
        .expect("`HistoryPruned` never disconnects");
    assert_eq!(state.unproductive_requests, 0);
    assert!(!state.session.cancel_token().is_cancelled());
    while let Ok(action) = actions.try_recv() {
        assert!(
            matches!(action, HeaderPortOperation::QueryHeaderLocator { .. }),
            "`HistoryPruned` schedules no penalty: {action:?}"
        );
    }
}

/// The schema 1 fallback holds across later rounds and routine `Status`
/// refreshes, starts at once, and costs the peer nothing.
#[test]
fn history_pruned_downgrades_the_peer_across_rounds_and_status_refreshes() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);

    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.tree_aux_schema, AuxSchema::V1);
    end(
        &mut reactor,
        &peer,
        &open,
        HeadersOutcomeCode::HistoryPruned,
    );

    let mut open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(
        (open.operation, open.tree_aux_schema),
        (v9::Operation::Open, AuxSchema::None),
        "the peer reopens at once without records"
    );
    for _ in 0..3 {
        end(
            &mut reactor,
            &peer,
            &open,
            HeadersOutcomeCode::SubscriptionSuperseded,
        );
        assert!(
            actions.try_recv().is_err(),
            "another outcome waits for status"
        );
        refresh(&mut reactor, &peer, &status);
        open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
        assert_eq!(
            open.tree_aux_schema,
            AuxSchema::None,
            "a routine status refresh keeps the fallback"
        );
    }
    assert_unpunished(&reactor, &mut actions, &peer);
}

/// Schema 1 returns only after the backoff, which doubles after each failed
/// probe up to its cap, and a failed probe never retries schema 1 at once.
#[tokio::test(start_paused = true)]
async fn schema_one_is_probed_again_after_a_bounded_backoff() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);
    refresh(&mut reactor, &peer, &status);
    let mut open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);

    let second = std::time::Duration::from_secs(1);
    let mut delays = Vec::new();
    for _ in 0..6 {
        assert_eq!(
            open.tree_aux_schema,
            AuxSchema::V1,
            "a probe asks for records"
        );
        end(
            &mut reactor,
            &peer,
            &open,
            HeadersOutcomeCode::HistoryPruned,
        );
        let fallback = reactor.peer_state[&peer]
            .aux_fallback
            .expect("a failed probe renews the fallback");
        delays.push(fallback.delay);
        open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
        assert_eq!(
            open.tree_aux_schema,
            AuxSchema::None,
            "a failed probe falls back at once"
        );

        time::advance(fallback.delay - second).await;
        end(
            &mut reactor,
            &peer,
            &open,
            HeadersOutcomeCode::SubscriptionSuperseded,
        );
        refresh(&mut reactor, &peer, &status);
        open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
        assert_eq!(
            open.tree_aux_schema,
            AuxSchema::None,
            "the backoff holds until it expires"
        );

        time::advance(second).await;
        end(
            &mut reactor,
            &peer,
            &open,
            HeadersOutcomeCode::SubscriptionSuperseded,
        );
        refresh(&mut reactor, &peer, &status);
        open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    }
    let minutes = |minutes: u64| std::time::Duration::from_secs(60 * minutes);
    assert_eq!(
        delays,
        [
            minutes(1),
            minutes(2),
            minutes(4),
            minutes(8),
            minutes(16),
            minutes(16)
        ]
    );
    assert_eq!(open.tree_aux_schema, AuxSchema::V1);
    assert_unpunished(&reactor, &mut actions, &peer);
}

/// A round without records that also ends `HistoryPruned` hands the target
/// to another peer, and the pruned peer waits out a backoff instead of
/// repeating the same attempt on its next status.
#[tokio::test(start_paused = true)]
async fn history_pruned_without_records_rotates_and_backs_off() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (pruned, mut pruned_outbound) = connect(&mut reactor, 0x71);
    let (other, mut other_outbound) = connect(&mut reactor, 0x72);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);
    refresh(&mut reactor, &pruned, &status);
    refresh(&mut reactor, &other, &status);

    let open = answer_locator(
        &mut reactor,
        &mut actions,
        &pruned,
        &mut pruned_outbound,
        target,
    );
    end(
        &mut reactor,
        &pruned,
        &open,
        HeadersOutcomeCode::HistoryPruned,
    );
    let open = answer_locator(
        &mut reactor,
        &mut actions,
        &pruned,
        &mut pruned_outbound,
        target,
    );
    assert_eq!(open.tree_aux_schema, AuxSchema::None);
    end(
        &mut reactor,
        &pruned,
        &open,
        HeadersOutcomeCode::HistoryPruned,
    );

    let rotated = answer_locator(
        &mut reactor,
        &mut actions,
        &other,
        &mut other_outbound,
        target,
    );
    end(
        &mut reactor,
        &other,
        &rotated,
        HeadersOutcomeCode::SubscriptionSuperseded,
    );
    refresh(&mut reactor, &pruned, &status);
    assert!(
        actions.try_recv().is_err(),
        "an unchanged status cannot repeat the pruned attempt"
    );

    time::advance(PRUNED_BACKOFF_MIN).await;
    refresh(&mut reactor, &pruned, &status);
    answer_locator(
        &mut reactor,
        &mut actions,
        &pruned,
        &mut pruned_outbound,
        target,
    );
    assert_unpunished(&reactor, &mut actions, &pruned);
}

/// `HistoryPruned` from a replaced session, or for a round that already
/// ended, leaves the peer's schema alone.
#[test]
fn a_stale_history_pruned_outcome_changes_nothing() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);
    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);

    reactor.handle_outcome(
        peer.clone(),
        1,
        v9::Ended {
            id: open.subscription_id,
            outcome: HeadersOutcomeCode::HistoryPruned,
            pages: 0,
            reached: false,
            accepted: open.acknowledged,
        },
    );
    assert!(reactor.peer_state[&peer].aux_fallback.is_none());
    assert!(reactor.peer_work_queue.active(&peer).is_some());

    end(
        &mut reactor,
        &peer,
        &open,
        HeadersOutcomeCode::SubscriptionSuperseded,
    );
    end(
        &mut reactor,
        &peer,
        &open,
        HeadersOutcomeCode::HistoryPruned,
    );
    assert!(reactor.peer_state[&peer].aux_fallback.is_none());
    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.tree_aux_schema, AuxSchema::V1);
}

/// The reopened round starts from the validated tip with a new `Open`. A page
/// that arrived but was never validated is dropped and refetched, and its
/// cursor is never acknowledged.
#[test]
fn reopening_after_history_pruned_drops_unvalidated_pages() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);
    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.tree_aux_schema, AuxSchema::V1);

    let tip = snapshot.frontiers.header_best;
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = tip.hash;
    header.time += chrono::Duration::seconds(1);
    reactor.deliver_page(
        peer.clone(),
        0,
        Headers {
            subscription_id: open.subscription_id,
            initial_target_tip_hash: target,
            common_ancestor_height: tip.height,
            common_ancestor_hash: tip.hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::V1,
            entries: vec![HeaderEntry {
                header: Arc::new(header),
                body_size: 0,
                tree_aux: Some(v9::test_support::record(
                    tip.height.next().expect("the page height fits"),
                )),
            }],
        },
    );
    assert_eq!(reactor.peer_work_queue.owned_header_count(&peer), 1);

    end(
        &mut reactor,
        &peer,
        &open,
        HeadersOutcomeCode::HistoryPruned,
    );
    assert_eq!(
        reactor.peer_work_queue.owned_header_count(&peer),
        0,
        "the unvalidated page is dropped"
    );
    let reopen = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(reopen.operation, v9::Operation::Open, "no Grant resumes it");
    assert_eq!(reopen.tree_aux_schema, AuxSchema::None);
    assert_eq!(
        (reopen.acknowledged.height, reopen.acknowledged.hash),
        (tip.height, tip.hash),
        "the new round starts at the validated tip"
    );
    assert_unpunished(&reactor, &mut actions, &peer);
}
