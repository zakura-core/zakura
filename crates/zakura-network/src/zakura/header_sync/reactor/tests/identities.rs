//! The reactor's work identities (M6a-4): each reservation's placement and
//! binding, each request's incarnation, and the leases a prepare or apply
//! call captures, as the observer sees them.
//!
//! Tests that call an inner handler run
//! [`HeaderSyncReactor::schedule_cached_statuses`] afterwards, as the reactor
//! loop does after every event.

use std::sync::Mutex as StdMutex;

use super::{
    aux_fallback::{answer_locator, next_update, pruned_status},
    status_offers::{ready_repair, status_for},
    *,
};
use crate::zakura::{
    header_sync::scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1, Frame, FramedRecv,
    FramedSend,
};
use observe::{HeaderSyncObserver, ObserverSlot};

/// Records every event in order.
#[derive(Debug, Default)]
pub(super) struct Recorder(StdMutex<Vec<ReactorEvent>>);

impl HeaderSyncObserver for Recorder {
    fn observe(&self, event: ReactorEvent) {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .push(event);
    }
}

impl Recorder {
    /// The events recorded since the last call.
    pub(super) fn take(&self) -> Vec<ReactorEvent> {
        std::mem::take(&mut *self.0.lock().expect("no test panics while holding it"))
    }
}

/// A reactor with an external port and `snapshot` committed, observed by
/// `recorder`. Its port actions arrive on the returned channel.
pub(super) fn observed(
    snapshot: &zakura_header_chain::EngineSnapshot,
    recorder: &Arc<Recorder>,
    direct: bool,
) -> (HeaderSyncReactor, mpsc::Receiver<HeaderPortOperation>) {
    let mut startup = startup(CancellationToken::new());
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    if direct {
        startup.header_chain_port =
            Arc::new(zakura_node_services::header_chain::InertHeaderChainPort);
        startup.use_direct_port();
    }
    startup.observer = ObserverSlot::new(recorder.clone());
    let (_, actions, reactor) =
        build_header_sync_reactor(startup).expect("the identity fixture starts");
    (reactor, actions)
}

/// The genesis snapshot the test reactors commit.
pub(super) fn genesis() -> zakura_header_chain::EngineSnapshot {
    let startup = startup(CancellationToken::new());
    committed_snapshot(zakura_header_chain::Frontier::new(
        startup.anchor.0,
        startup.anchor.1,
    ))
}

/// Connects the peer `byte`, keeping a sender into its outbound queue, and
/// drops its initial status.
pub(super) fn connect(
    reactor: &mut HeaderSyncReactor,
    byte: u8,
) -> (ZakuraPeerId, FramedSend, FramedRecv) {
    let peer = ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length");
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send.clone(),
        CancellationToken::new(),
    ));
    outbound.try_recv().expect("the initial status was sent");
    (peer, send, outbound)
}

/// `count` headers that extend `parent`, one second apart.
pub(super) fn headers(parent: zakura_header_chain::Frontier, count: u32) -> Vec<HeaderEntry> {
    let mut parent = parent.hash;
    (1..=count)
        .map(|second| {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent;
            header.time += chrono::Duration::seconds(i64::from(second));
            parent = header.hash();
            HeaderEntry {
                header: Arc::new(header),
                body_size: 0,
                tree_aux: None,
            }
        })
        .collect()
}

/// The reactor's state, settled now, as its observer receives it.
pub(super) fn settle(reactor: &HeaderSyncReactor, recorder: &Recorder) -> observe::ReactorSnapshot {
    recorder.take();
    reactor.settled(false);
    match recorder.take().as_slice() {
        [ReactorEvent::Settled { snapshot, .. }] => (**snapshot).clone(),
        other => panic!("one Settled, got {other:?}"),
    }
}

/// The identity events in `events`, with each request's start and
/// retirement.
fn identities(events: Vec<ReactorEvent>) -> Vec<ReactorEvent> {
    events
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                ReactorEvent::Placed { .. }
                    | ReactorEvent::Bound { .. }
                    | ReactorEvent::LeasesCaptured { .. }
                    | ReactorEvent::Started { .. }
                    | ReactorEvent::Retired { .. }
            )
        })
        .collect()
}

/// An ordinary target reserves, opens and binds its reservation to the
/// request's incarnation, which the settled snapshot lists with the
/// reservation the queue holds. An open the session's full queue refuses
/// shows its placement but no binding, and the next snapshot holds nothing
/// for the peer.
///
/// This fails if `start` skips the binding.
#[test]
fn a_reservation_is_placed_then_bound() {
    for refused in [false, true] {
        let snapshot = genesis();
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, mut actions) = observed(&snapshot, &recorder, false);
        let (peer, send, mut outbound) = connect(&mut reactor, 0x71);
        let target = block::Hash([0x53; 32]);
        let status = pruned_status(&snapshot, target);
        reactor.handle_status(peer.clone(), 0, status);
        reactor.schedule_cached_statuses();
        let scope = match actions.try_recv() {
            Ok(HeaderPortOperation::QueryHeaderLocator { scope, .. }) => scope,
            other => panic!("expected the locator query, got {other:?}"),
        };
        if refused {
            let filler = Frame {
                message_type: 1,
                flags: 0,
                payload: Vec::new(),
            };
            while send.try_send(filler.clone()).is_ok() {}
        }
        recorder.take();
        reactor.handle_header_locator_ready(
            peer.clone(),
            0,
            target,
            scope,
            Some(zakura_header_chain::HeaderLocator::for_continuation(
                snapshot.frontiers.header_best,
            )),
        );
        reactor.schedule_cached_statuses();

        let events = identities(recorder.take());
        let Some(ReactorEvent::Placed {
            reservation,
            peer: placed_for,
            count,
            placement,
        }) = events.first().cloned()
        else {
            panic!("the open reserved first: {events:?}");
        };
        assert_eq!(placed_for, peer);
        assert!(count > 0);
        assert_eq!(
            placement,
            Some(observe::Placement {
                session_id: 0,
                scope,
                target_tip_hash: target,
            }),
            "it's placed for the offer it opens"
        );
        let settled = settle(&reactor, &recorder);
        if refused {
            assert!(
                reactor.peer_work_queue.active(&peer).is_none(),
                "the full queue refused the open"
            );
            assert_eq!(events.len(), 1, "no binding and no start: {events:?}");
            assert!(settled.holdings.is_empty() && settled.work.is_empty());
            continue;
        }
        let incarnation = reactor
            .peer_work_queue
            .active(&peer)
            .expect("the request started")
            .incarnation;
        assert_eq!(incarnation, 1, "the queue's first incarnation");
        assert!(
            matches!(
                &events[1..],
                [
                    ReactorEvent::Bound {
                        reservation: bound,
                        incarnation: 1,
                        via: observe::Binding::Start,
                    },
                    ReactorEvent::Started { request, .. },
                ] if *bound == reservation && request.incarnation == 1
            ),
            "the start binds the reservation, then reports its incarnation: {events:?}"
        );
        assert_eq!(next_update(&mut outbound).operation, v9::Operation::Open);
        assert_eq!(
            settled.holdings,
            [observe::HoldingEntry {
                peer: peer.clone(),
                reservation: Some(reservation),
                leases: Vec::new(),
            }]
        );
        assert!(matches!(
            settled.work[..],
            [observe::WorkEntry {
                phase: observe::WorkPhase::Active { incarnation: 1, .. },
                ..
            }]
        ));
    }
}

/// One incarnation takes two reservation windows: its first page spends
/// the first, and the continuation reserves the second, which binds at once
/// to the same incarnation. The snapshot then holds the new reservation and
/// the page's lease.
///
/// This fails if the continuation binds to a new incarnation.
#[test]
fn a_continuation_reserves_for_its_own_incarnation() {
    let snapshot = genesis();
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, mut actions) = observed(&snapshot, &recorder, false);
    let (peer, _send, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let mut status = pruned_status(&snapshot, target);
    status.work_anchor_height = snapshot.frontiers.finalized.height;
    status.selected_tip_height = block::Height(100);
    status.max_headers_per_response = 2;
    reactor.handle_status(peer.clone(), 0, status);
    reactor.schedule_cached_statuses();
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    assert_eq!(open.added_credit.objects, 2);

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
            entries: headers(snapshot.frontiers.header_best, 2),
        },
    );
    reactor.schedule_cached_statuses();
    assert_eq!(
        next_update(&mut outbound).operation,
        v9::Operation::Grant,
        "the spent window continued with a grant"
    );

    let events = identities(recorder.take());
    let placed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Placed { reservation, .. } => Some(*reservation),
            _ => None,
        })
        .collect();
    let bound: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Bound {
                reservation,
                incarnation,
                via,
            } => Some((*reservation, *incarnation, *via)),
            _ => None,
        })
        .collect();
    assert_eq!(placed.len(), 2, "two windows: {events:?}");
    assert_ne!(placed[0], placed[1]);
    assert_eq!(
        bound,
        [
            (placed[0], 1, observe::Binding::Start),
            (placed[1], 1, observe::Binding::Continuation)
        ]
    );
    let settled = settle(&reactor, &recorder);
    let [holding] = &settled.holdings[..] else {
        panic!("one peer's holdings: {:?}", settled.holdings);
    };
    assert_eq!(holding.reservation, Some(placed[1]));
    assert_eq!(holding.leases.len(), 1);
    assert_eq!(holding.leases[0].count, 2);
    assert!(matches!(
        settled.work[..],
        [observe::WorkEntry {
            phase: observe::WorkPhase::Active {
                incarnation: 1,
                held: 2,
                ..
            },
            ..
        }]
    ));
}

/// A subscription that stopped at a committed prefix runs a second round
/// under the same subscription and request ID. Each round is its own
/// incarnation, with its own reservation bound to it.
///
/// This fails if incarnations are keyed by subscription ID.
#[test]
fn two_rounds_on_one_subscription_stay_apart() {
    let mut snapshot = genesis();
    let anchor = snapshot.frontiers.finalized;
    // Two headers of durable headroom make each round a two-header prefix.
    let max_nodes = u32::try_from(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        .expect("the retained-node limit fits a height");
    snapshot.frontiers.header_best = zakura_header_chain::Frontier::new(
        block::Height(anchor.height.0 + max_nodes - 2),
        block::Hash([0x51; 32]),
    );
    snapshot.frontiers.verified_best = snapshot.frontiers.header_best;
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, mut actions) = observed(&snapshot, &recorder, false);
    let (peer, _send, mut outbound) = connect(&mut reactor, 0x71);
    let mut rounds = Vec::new();
    // The publisher's tip stays put, so the second round continues the
    // first's subscription from its accepted prefix.
    let target = block::Hash([0x53; 32]);
    // Where each round starts: the committed tip, then the prefix the last
    // round committed, which its locator names.
    let mut accepted = snapshot.frontiers.header_best;
    for round in 0..2 {
        let mut status = pruned_status(&snapshot, target);
        status.work_anchor_height = anchor.height;
        status.selected_tip_height = block::Height(snapshot.frontiers.header_best.height.0 + 100);
        reactor.handle_status(peer.clone(), 0, status);
        reactor.schedule_cached_statuses();
        let scope = match actions.try_recv() {
            Ok(HeaderPortOperation::QueryHeaderLocator { scope, .. }) => scope,
            other => panic!("expected round {round}'s locator query, got {other:?}"),
        };
        reactor.handle_header_locator_ready(
            peer.clone(),
            0,
            target,
            scope,
            Some(zakura_header_chain::HeaderLocator::for_continuation(
                accepted,
            )),
        );
        reactor.schedule_cached_statuses();
        let update = next_update(&mut outbound);
        assert_eq!(
            update.operation,
            [v9::Operation::Open, v9::Operation::Grant][round],
            "the second round resumes the subscription"
        );
        let active = reactor
            .peer_work_queue
            .active(&peer)
            .expect("the round started")
            .clone();
        rounds.push((active.request_id, active.incarnation));
        let entries = headers(accepted, 2);
        reactor.deliver_page(
            peer.clone(),
            0,
            Headers {
                subscription_id: update.subscription_id,
                initial_target_tip_hash: target,
                common_ancestor_height: accepted.height,
                common_ancestor_hash: accepted.hash,
                reaches_initial_target: false,
                tree_aux_schema: AuxSchema::None,
                entries: entries.clone(),
            },
        );
        accepted = zakura_header_chain::Frontier::new(
            block::Height(accepted.height.0 + 2),
            entries[1].header.hash(),
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
        reactor.schedule_cached_statuses();
        assert!(
            reactor.peer_state[&peer]
                .session
                .subscriber()
                .resumable()
                .is_some(),
            "the subscription is kept for the next round"
        );
    }

    assert_eq!(rounds[0].0, rounds[1].0, "both rounds share one request ID");
    assert_eq!((rounds[0].1, rounds[1].1), (1, 2));
    let events = identities(recorder.take());
    let bindings: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Bound { incarnation, .. } => Some(*incarnation),
            _ => None,
        })
        .collect();
    let starts: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Started { request, .. } => {
                Some((request.request_id, request.incarnation))
            }
            _ => None,
        })
        .collect();
    assert_eq!(bindings, [1, 2], "each round binds its own reservation");
    assert_eq!(starts, [(rounds[0].0.get(), 1), (rounds[1].0.get(), 2)]);
}

/// A bounded prefix rebinds an incarnation's authority to the staged tip.
/// The prepare call's owner is the rebound one, the call's captured leases
/// name the same incarnation and the leases the queue holds for it, and the
/// next settled snapshot lists the incarnation under the rebound owner.
///
/// This fails if the snapshot keeps the incarnation's first authority.
#[test]
fn a_rebound_owner_follows_the_prefix() {
    let snapshot = genesis();
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, _actions) = observed(&snapshot, &recorder, true);
    let peer = super::peer();
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send,
        CancellationToken::new(),
    ));
    let (_, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 0);
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let incarnation = active.incarnation;
    let entry = active.entries[0].clone();
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = Some(snapshot.frontiers.finalized);
    active.entries = vec![entry; HEADER_CHUNK_BUDGET_CAPACITY_V1 - 1];
    active.max_header_count = 1;
    let staged_tip = active
        .staged_tip()
        .expect("the bounded staged fixture has an inferred tip");
    let mut next_header = *regtest_genesis_block().header;
    next_header.previous_block_hash = staged_tip.hash;
    let _ = active;
    reactor
        .peer_work_queue
        .set_capacity_for_test(&peer, HEADER_CHUNK_BUDGET_CAPACITY_V1 - 1, 1);
    recorder.take();

    // The last page fills the budget, so the staged prefix is prepared.
    reactor.deliver_page(
        peer.clone(),
        owner.session_id(),
        Headers {
            subscription_id: owner.request_id().get(),
            initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: staged_tip.height,
            common_ancestor_hash: staged_tip.hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: Arc::new(next_header),
                body_size: 0,
                tree_aux: None,
            }],
        },
    );

    let events = recorder.take();
    let Some((call, prepared)) = events.iter().find_map(|event| match event {
        ReactorEvent::Dispatched {
            call,
            op: observe::PortCall::Prepare { owner, .. },
        } => Some((*call, *owner)),
        _ => None,
    }) else {
        panic!("the full budget prepared the prefix: {events:?}");
    };
    assert_ne!(prepared, owner, "the prefix rebound the authority");
    assert_eq!(
        prepared.header_authority().branch.target_tip_hash,
        reactor
            .peer_work_queue
            .active(&peer)
            .unwrap()
            .owner
            .header_authority()
            .branch
            .target_tip_hash,
    );
    let settled = settle(&reactor, &recorder);
    let leases: Vec<_> = settled.holdings[0]
        .leases
        .iter()
        .map(|lease| lease.lease)
        .collect();
    assert_eq!(leases.len(), 2, "the seeded lease and the page's");
    assert!(
        events.contains(&ReactorEvent::LeasesCaptured {
            call,
            incarnation: Some(incarnation),
            leases,
        }),
        "the call captured the incarnation's leases: {events:?}"
    );
    assert!(
        matches!(
            settled.work[..],
            [observe::WorkEntry {
                phase: observe::WorkPhase::Active {
                    incarnation: listed,
                    owner: listed_owner,
                    ..
                },
                ..
            }] if listed == incarnation && listed_owner == prepared
        ),
        "the snapshot lists the rebound owner: {:?}",
        settled.work
    );
}

/// A repair reserves before it stages its work, so it names the work it
/// reserves for: the supplier's session, the repair's scope and the round's
/// target, the same that its request then starts with. When the open fails,
/// that placement is all that's left, with no binding or start.
///
/// This fails if the queue ignores the repair's named placement.
#[test]
fn a_failed_repair_open_keeps_its_placement() {
    let mut started = None;
    for refused in [false, true] {
        let mut snapshot = genesis();
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, mut actions) = observed(&snapshot, &recorder, false);
        reactor.startup.committed_snapshots = None;
        snapshot.frontiers.header_best =
            zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x40; 32]));
        reactor.observe_latest_committed_snapshot(snapshot.clone());
        let (supplier, send, _outbound) = connect(&mut reactor, 0x71);
        let mut status = status_for(&snapshot, block::Hash([0x42; 32]));
        status.tree_aux_schema_mask = AuxSchema::V1.mask_bit();
        reactor.handle_status(supplier.clone(), 0, status);
        assert!(matches!(
            actions.try_recv(),
            Ok(HeaderPortOperation::QueryHeaderLocator { .. })
        ));
        if refused {
            let filler = Frame {
                message_type: 1,
                flags: 0,
                payload: Vec::new(),
            };
            while send.try_send(filler.clone()).is_ok() {}
        }
        recorder.take();
        ready_repair(&mut reactor, &mut actions, &snapshot);

        let events = identities(recorder.take());
        let Some(ReactorEvent::Placed {
            peer, placement, ..
        }) = events.first().cloned()
        else {
            panic!("the repair reserved first: {events:?}");
        };
        assert_eq!(peer, supplier);
        let placement = placement.expect("the repair names its placement");
        if !refused {
            let Some(ReactorEvent::Started { request, purpose }) = events.get(2) else {
                panic!("the repair bound and started: {events:?}");
            };
            assert!(matches!(
                purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            ));
            assert_eq!(
                placement,
                observe::Placement {
                    session_id: request.session_id,
                    scope: request.owner.header_authority(),
                    target_tip_hash: request.target_tip_hash,
                },
                "it's placed for the work it starts"
            );
            started = Some(placement);
            continue;
        }
        assert!(
            reactor.peer_work_queue.active(&supplier).is_none(),
            "the full queue refused the repair's open"
        );
        assert_eq!(events.len(), 1, "no binding and no start: {events:?}");
        assert_eq!(
            Some(placement),
            started,
            "the refused repair names the same work"
        );
        assert!(settle(&reactor, &recorder).holdings.is_empty());
    }
}
