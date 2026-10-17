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
pub(super) fn fallback_reactor() -> (
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
pub(super) fn connect(reactor: &mut HeaderSyncReactor, byte: u8) -> (ZakuraPeerId, FramedRecv) {
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
pub(super) fn pruned_status(
    snapshot: &zakura_header_chain::EngineSnapshot,
    target: block::Hash,
) -> Status {
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
pub(super) fn refresh(reactor: &mut HeaderSyncReactor, peer: &ZakuraPeerId, status: &Status) {
    reactor.handle_status(peer.clone(), 0, status.clone());
    reactor.schedule_cached_statuses();
}

/// Answer `peer`'s locator query for `target` with the validated tip, and
/// return the subscription update the round sends.
pub(super) fn answer_locator(
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
pub(super) fn next_update(outbound: &mut FramedRecv) -> v9::SubscribeHeaders {
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
pub(super) fn end(
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

/// The fallback belongs to the peer, not its session. It holds across a
/// disconnect and reconnect, so session churn cannot reset the backoff.
#[test]
fn the_fallback_outlives_a_reconnect() {
    let (mut reactor, mut actions, snapshot) = fallback_reactor();
    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let status = pruned_status(&snapshot, target);
    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.tree_aux_schema, AuxSchema::V1);
    reactor.deliver_outcome(
        peer.clone(),
        0,
        HeadersOutcome {
            subscription_id: open.subscription_id,
            initial_target_tip_hash: target,
            outcome: HeadersOutcomeCode::HistoryPruned,
        },
    );
    reactor.handle_peer_disconnected(&peer, 0, "test");

    let (peer, mut outbound) = connect(&mut reactor, 0x71);
    refresh(&mut reactor, &peer, &status);
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(
        open.tree_aux_schema,
        AuxSchema::None,
        "a new session keeps the peer's fallback"
    );
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
        let fallback = reactor.pruned_peers[&peer]
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
    assert!(reactor
        .pruned_peers
        .get(&peer)
        .is_none_or(|pruned| pruned.aux_fallback.is_none()));
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
    assert!(reactor
        .pruned_peers
        .get(&peer)
        .is_none_or(|pruned| pruned.aux_fallback.is_none()));
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

/// A tip status from a publisher that finalized through `tip`, serves both
/// schemas, and sends pages of at most four headers, so rounds stay small.
fn tip_status(tip: zakura_header_chain::Frontier) -> Status {
    Status {
        work_anchor_height: tip.height,
        work_anchor_hash: tip.hash,
        selected_tip_height: tip.height,
        selected_tip_hash: tip.hash,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 4,
        max_subscriptions: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
    }
}

/// A pruned publisher's chain: it holds `tree` but no schema 1 record at or
/// below `index_height`, as below a pruned node's VCT index.
struct PrunedChain<'a> {
    tree: &'a v9::test_support::HeaderTree,
    index_height: u32,
}

/// Serve `open`, `peer`'s round toward `target`, as a publisher of `chain`. A schema
/// 1 round that starts there ends `HistoryPruned`. Any other round gets pages
/// within its credit, and the next after each `Grant`, until one reaches the
/// target, which state then admits and commits. Returns whether state
/// admitted the target.
fn serve_pruned(
    reactor: &mut HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    peer: &ZakuraPeerId,
    outbound: &mut FramedRecv,
    chain: &PrunedChain<'_>,
    target: zakura_header_chain::Frontier,
    open: &v9::SubscribeHeaders,
) -> bool {
    let PrunedChain { tree, index_height } = *chain;
    assert_eq!(open.operation, v9::Operation::Open);
    let schema = open.tree_aux_schema;
    let start = tree.nodes[&open.locator_hashes[0]].0;
    if schema == AuxSchema::V1 && start.height.0 < index_height {
        end(reactor, peer, open, HeadersOutcomeCode::HistoryPruned);
        return false;
    }

    let mut credit = open.added_credit.objects;
    let mut parent = start;
    loop {
        let last = parent.height.0 + credit.min(target.height.0 - parent.height.0);
        let entries = (parent.height.0 + 1..=last)
            .map(|height| {
                let at = tree
                    .ancestor(target, block::Height(height))
                    .expect("the tree holds the path");
                HeaderEntry {
                    header: tree.nodes[&at.hash].1.clone(),
                    body_size: 0,
                    tree_aux: (schema == AuxSchema::V1)
                        .then(|| v9::test_support::record(at.height)),
                }
            })
            .collect::<Vec<_>>();
        credit -= u32::try_from(entries.len()).expect("a page fits u32");
        reactor.deliver_page(
            peer.clone(),
            0,
            Headers {
                subscription_id: open.subscription_id,
                initial_target_tip_hash: target.hash,
                common_ancestor_height: parent.height,
                common_ancestor_hash: parent.hash,
                reaches_initial_target: last == target.height.0,
                tree_aux_schema: schema,
                entries,
            },
        );
        parent = tree
            .ancestor(target, block::Height(last))
            .expect("the tree holds the path");
        if parent == target {
            break;
        }
        let grant = next_update(outbound);
        assert_eq!(grant.operation, v9::Operation::Grant, "the round continues");
        credit += grant.added_credit.objects;
    }

    let HeaderPortOperation::PrepareHeaderTarget {
        source,
        owner,
        target: prepared,
        ..
    } = actions.try_recv().expect("the target page is prepared")
    else {
        panic!("a page that reaches the target prepares it");
    };
    assert_eq!(prepared, target);
    reactor
        .peer_work_queue
        .active_mut(peer)
        .expect("the round awaits admission")
        .phase = HeaderTargetPhase::Applying;
    reactor.handle_header_target_admission_ready(
        peer.clone(),
        source,
        owner,
        HeaderTargetAdmissionResult::Applied,
    );
    assert_eq!(next_update(outbound).operation, v9::Operation::Close);
    end(reactor, peer, open, HeadersOutcomeCode::SubscriptionClosed);
    commit_tip(reactor, target);
    true
}

/// Serve every ordinary round the reactor starts, each from a publisher of
/// `chain` as [`serve_pruned`] describes, until the committed header tip
/// reaches `goal` and no query is left. Returns each round's peer, schema,
/// and whether state admitted it. A locator query for a tip this node
/// already passed gets no locator.
fn drive_to(
    reactor: &mut HeaderSyncReactor,
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    outbounds: &mut HashMap<ZakuraPeerId, FramedRecv>,
    chain: &PrunedChain<'_>,
    goal: zakura_header_chain::Frontier,
) -> Vec<(ZakuraPeerId, AuxSchema, bool)> {
    let mut rounds = Vec::new();
    for _ in 0..32 {
        let best = reactor
            .committed_snapshot
            .as_ref()
            .expect("the fixture has a committed snapshot")
            .frontiers
            .header_best;
        let action = actions.try_recv();
        if best == goal && action.is_err() {
            return rounds;
        }
        let Ok(HeaderPortOperation::QueryHeaderLocator {
            peer,
            target_tip_hash,
            scope,
            ..
        }) = action
        else {
            panic!("header sync stalled at {best:?} before {goal:?}: {action:?}");
        };
        let target = chain.tree.nodes[&target_tip_hash].0;
        if target.height <= best.height {
            reactor.handle_header_locator_ready(peer, 0, target_tip_hash, scope, None);
            reactor.schedule_cached_statuses();
            continue;
        }
        reactor.handle_header_locator_ready(
            peer.clone(),
            0,
            target_tip_hash,
            scope,
            Some(zakura_header_chain::HeaderLocator::for_continuation(best)),
        );
        let outbound = outbounds.get_mut(&peer).expect("the peer is connected");
        let open = next_update(outbound);
        let admitted = serve_pruned(reactor, actions, &peer, outbound, chain, target, &open);
        rounds.push((peer, open.tree_aux_schema, admitted));
    }
    panic!("header sync did not reach {goal:?}");
}

/// Commit `tip` as the selected header tip, as state does after admission.
fn commit_tip(reactor: &mut HeaderSyncReactor, tip: zakura_header_chain::Frontier) {
    let mut snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    snapshot.frontiers.header_best = tip;
    snapshot.state_version = snapshot
        .state_version
        .checked_next()
        .expect("the state version has a successor");
    snapshot.header_generation = snapshot
        .header_generation
        .checked_next()
        .expect("the header generation has a successor");
    reactor.observe_latest_committed_snapshot(snapshot);
    reactor.schedule_cached_statuses();
}

/// Two pruned publishers without schema 1 records at or below height 6
/// bring a node from genesis past that height without stalling. Each
/// schema 1 round from below the index height ends `HistoryPruned`, rounds
/// without records carry the chain past it, and no peer asks for records
/// again within its backoff. Root repair asks a fallen-back peer for schema 1.
/// After the backoff, a schema 1 probe that starts above the index height
/// gets its records and ends that peer's fallback.
#[tokio::test(start_paused = true)]
async fn pruned_publishers_bring_a_node_from_genesis_past_their_index_height() {
    let (mut reactor, mut actions, _snapshot) = fallback_reactor();
    let mut outbounds = HashMap::new();
    let mut peers = Vec::new();
    for byte in [0x71, 0x72] {
        let (peer, outbound) = connect(&mut reactor, byte);
        outbounds.insert(peer.clone(), outbound);
        peers.push(peer);
    }
    let mut tree = v9::test_support::HeaderTree::new();
    let chain = tree.extend(tree.genesis, 16);
    let pruned = PrunedChain {
        tree: &tree,
        index_height: 6,
    };

    // Both advertise height 4, and the first peer takes the target. Its
    // schema 1 round from genesis ends `HistoryPruned`, and its round
    // without records reaches the tip.
    for peer in &peers {
        refresh(&mut reactor, peer, &tip_status(chain[3]));
    }
    let rounds = drive_to(
        &mut reactor,
        &mut actions,
        &mut outbounds,
        &pruned,
        chain[3],
    );
    assert_eq!(
        rounds,
        [
            (peers[0].clone(), AuxSchema::V1, false),
            (peers[0].clone(), AuxSchema::None, true),
        ]
    );

    // The second peer advertises height 10, falls back the same way, and
    // carries the chain past the index height.
    refresh(&mut reactor, &peers[1], &tip_status(chain[9]));
    let rounds = drive_to(
        &mut reactor,
        &mut actions,
        &mut outbounds,
        &pruned,
        chain[9],
    );
    assert_eq!(
        rounds,
        [
            (peers[1].clone(), AuxSchema::V1, false),
            (peers[1].clone(), AuxSchema::None, true),
        ]
    );
    let fallen_back = peers.clone();
    for peer in &fallen_back {
        assert!(reactor.pruned_peers[peer].aux_fallback.is_some());
    }

    // Root repair still asks a fallen-back peer for schema 1.
    reactor.observe_vct_root_repair(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: chain[4].height,
        },
        generation: 1,
    });
    let HeaderPortOperation::QueryVctRepairContext { owner, .. } =
        actions.try_recv().expect("the repair asks for its context")
    else {
        panic!("the repair must request its context");
    };
    reactor.handle_vct_repair_context_ready(
        owner,
        VctRepairContextResult::Resolved(zakura_header_chain::VctRepairContext::unconstrained(
            chain[4],
            zakura_header_chain::HeaderLocator::for_continuation(chain[3]),
            None,
        )),
    );
    let supplier = fallen_back
        .iter()
        .find(|peer| reactor.peer_work_queue.active(peer).is_some())
        .expect("a fallen-back peer supplies the repair")
        .clone();
    let repair = reactor
        .peer_work_queue
        .active(&supplier)
        .expect("the supplier owns the repair")
        .clone();
    assert!(matches!(
        repair.purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
    ));
    let supplier_outbound = outbounds.get_mut(&supplier).expect("the peer is connected");
    let repair_open = next_update(supplier_outbound);
    assert_eq!(
        (repair.tree_aux_schema, repair_open.tree_aux_schema),
        (AuxSchema::V1, AuxSchema::V1),
        "repair never takes the ordinary fallback"
    );
    reactor.observe_vct_root_repair(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Idle,
        generation: 2,
    });
    assert_eq!(
        next_update(supplier_outbound).operation,
        v9::Operation::Close
    );
    end(
        &mut reactor,
        &supplier,
        &repair_open,
        HeadersOutcomeCode::SubscriptionClosed,
    );

    // After the backoff, the next round probes schema 1 from above the index
    // height, gets its records, and ends that peer's fallback.
    time::advance(PRUNED_BACKOFF_MAX).await;
    let prober = fallen_back[0].clone();
    refresh(&mut reactor, &prober, &tip_status(chain[15]));
    let rounds = drive_to(
        &mut reactor,
        &mut actions,
        &mut outbounds,
        &pruned,
        chain[15],
    );
    assert_eq!(rounds, [(prober.clone(), AuxSchema::V1, true)]);
    assert!(reactor
        .pruned_peers
        .get(&prober)
        .is_none_or(|pruned| pruned.aux_fallback.is_none()));
    for peer in &peers {
        assert_unpunished(&reactor, &mut actions, peer);
    }
}
