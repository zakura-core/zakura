//! The lifetime of a call's retained headers, through the real reactor.
//!
//! Each history also folds through [`observe::HeaderWorkLedger`], so a test that
//! pins one call's ownership also holds the ledger to the real event order.

use observe::{HeaderWorkEnd, HeaderWorkLedger};

use super::*;
use crate::zakura::header_sync::scheduler::peer_work::{RetainedHeaders, StorageCall};

/// A reactor on `port` that records to a new [`Recorder`].
fn recorded(
    port: Arc<ScriptedPort>,
) -> (
    HeaderSyncReactor,
    zakura_header_chain::EngineSnapshot,
    Arc<Recorder>,
) {
    let recorder = Arc::new(Recorder::default());
    let (reactor, snapshot) = observed_reactor(
        port,
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    (reactor, snapshot, recorder)
}

/// Starts a round for `peer` in the preparing phase, one header owned and one
/// reserved, and reports its `Started`, which the seed does not.
fn seed_preparing(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    recorder: &Recorder,
    peer: &ZakuraPeerId,
    session_id: u64,
) -> ActiveHeaderRequest {
    seed_applying_request(reactor, snapshot, peer.clone(), session_id);
    let round = reactor
        .peer_work_queue
        .active_mut(peer)
        .expect("the seed started this round");
    round.phase = HeaderTargetPhase::Preparing;
    let round = round.clone();
    recorder.observe(ReactorEvent::started(&round));
    round
}

/// The prepare call for `round`, with its own entries.
fn prepare(reactor: &HeaderSyncReactor, round: &ActiveHeaderRequest) -> HeaderPortOperation {
    prepare_operation(
        reactor,
        &round.peer,
        HeaderTargetPurpose::Normal,
        round.entries.clone(),
    )
}

/// The target a prepare for `peer`'s round seals.
fn sealed(reactor: &HeaderSyncReactor, peer: &ZakuraPeerId) -> port::PreparedHeaderTarget {
    let HeaderPortOperation::ApplyHeaderTarget { target, .. } = apply_operation(reactor, peer)
    else {
        unreachable!("`apply_operation` always builds an apply")
    };
    target
}

/// Headers `peer`'s current round retains for the call `number`, without a
/// `Dispatched`, for a test that drives one completion handler directly.
fn held_by_prepare(
    reactor: &mut HeaderSyncReactor,
    peer: &ZakuraPeerId,
    number: u64,
) -> RetainedHeaders {
    let mut retained = reactor
        .peer_work_queue
        .retain_headers(peer)
        .expect("the peer has a started round");
    retained.observed_by(&reactor.startup.observer);
    retained.enter_call(StorageCall {
        number,
        kind: StorageCallKind::Prepare,
    });
    let work = retained.work().expect("the call was just entered");
    reactor.peer_work_queue.bind_call(peer, work);
    retained
}

/// Only `events`' ownership facts, as `(call, how it ended)`, `None` for a capture.
fn ownership(events: &[ReactorEvent]) -> Vec<(u64, Option<HeaderWorkEnd>)> {
    events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::HeadersHeld { call, .. } => Some((*call, None)),
            ReactorEvent::HeadersEnded { call, end, .. } => Some((*call, Some(*end))),
            _ => None,
        })
        .collect()
}

/// Folds `events` through the ledger, and requires them clean.
fn ledger_accepts(events: &[ReactorEvent]) -> HeaderWorkLedger {
    let mut ledger = HeaderWorkLedger::default();
    for event in events {
        ledger
            .observe(event)
            .unwrap_or_else(|broken| panic!("{broken}\n{events:#?}"));
    }
    ledger
}

/// A sealed prepare hands its headers to its apply: nothing releases between
/// the two calls, and the apply's end is what releases them.
#[tokio::test]
async fn a_prepare_hands_its_headers_to_its_apply_without_a_release() {
    let gate = Arc::new(TokioSemaphore::new(0));
    let scripted = Arc::new(ScriptedPort {
        gate: Some(gate.clone()),
        applies: true,
        ..ScriptedPort::default()
    });
    let (mut reactor, snapshot, recorder) = recorded(scripted.clone());
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    scripted
        .prepared
        .lock()
        .expect("unpoisoned")
        .push(sealed(&reactor, &peer));
    assert!(matches!(
        reactor.dispatch_action(prepare(&reactor, &round)),
        StorageDispatch::Started(_)
    ));
    let charged = reactor.peer_work_queue.chunk_budget_usage();
    let mut releases = reactor.peer_work_queue.capacity_releases();
    releases.mark_unchanged();

    gate.add_permits(1);
    complete_next(&mut reactor).await;

    assert_eq!(reactor.peer_work_queue.chunk_budget_usage(), charged);
    assert!(
        !releases.has_changed().expect("the budget is alive"),
        "the handoff releases nothing"
    );
    let handed = recorder.take_all();
    assert_eq!(
        ownership(&handed),
        vec![
            (1, None),
            (1, Some(HeaderWorkEnd::HandedToApply { apply: 2 })),
            (2, None),
        ]
    );
    assert!(handed.contains(&ReactorEvent::HeadersHeld {
        call: 2,
        kind: StorageCallKind::Apply,
        peer: peer.clone(),
        incarnation: round.incarnation.get(),
        headers: 1,
    }));

    gate.add_permits(1);
    complete_next(&mut reactor).await;

    let applied = recorder.take_all();
    assert_eq!(ownership(&applied), vec![(2, Some(HeaderWorkEnd::Settled))]);
    let history: Vec<_> = handed.into_iter().chain(applied).collect();
    ledger_accepts(&history).finish().expect("every call ended");
}

/// A call from an earlier round cannot settle a later round with the same owner,
/// and its headers stay charged until the call ends.
#[tokio::test]
async fn an_earlier_rounds_prepare_cannot_settle_a_later_round_with_the_same_owner() {
    let gate = Arc::new(TokioSemaphore::new(0));
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort {
        gate: Some(gate.clone()),
        ..ScriptedPort::default()
    }));
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let old = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    assert!(matches!(
        reactor.dispatch_action(prepare(&reactor, &old)),
        StorageDispatch::Started(_)
    ));
    reactor.retire_peer_work(&peer, HeaderRequestTerminal::TimedOut);
    assert_eq!(
        reactor.peer_work_queue.chunk_budget_usage(),
        (0, 1),
        "the timed-out round's call keeps its header charged"
    );

    let new = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    assert_eq!(new.owner, old.owner, "a resumed round reuses its owner");
    assert_ne!(new.incarnation, old.incarnation);
    assert!(matches!(
        reactor.dispatch_action(prepare(&reactor, &new)),
        StorageDispatch::Started(_)
    ));

    gate.add_permits(1);
    complete_next(&mut reactor).await;

    assert_eq!(
        active(&reactor, &peer),
        new,
        "the earlier call's failure leaves the later round alone"
    );
    assert_eq!(reactor.peer_work_queue.chunk_budget_usage(), (1, 1));

    gate.add_permits(1);
    complete_next(&mut reactor).await;

    let events = recorder.take_all();
    assert_eq!(
        ownership(&events),
        vec![
            (1, None),
            (2, None),
            (1, Some(HeaderWorkEnd::Stale)),
            (2, Some(HeaderWorkEnd::PrepareFailed)),
        ]
    );
    ledger_accepts(&events).finish().expect("every call ended");
    assert_eq!(reactor.peer_work_queue.chunk_budget_usage(), (0, 0));
}

/// A prepare the reactor answers itself still ends the headers it held.
#[tokio::test]
async fn a_prepare_that_storage_never_sees_ends_its_headers() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort::default()));
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    let oversized = prepare_operation(
        &reactor,
        &peer,
        HeaderTargetPurpose::Normal,
        vec![round.entries[0].clone(); zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1 + 1],
    );

    assert!(matches!(
        reactor.dispatch_action(oversized),
        StorageDispatch::Started(_)
    ));
    complete_next(&mut reactor).await;

    let events = recorder.take_all();
    assert!(events.contains(&ReactorEvent::CallEnded {
        call: 1,
        end: CallEnd::NotSubmitted,
    }));
    assert_eq!(
        ownership(&events),
        vec![(1, None), (1, Some(HeaderWorkEnd::PrepareFailed))]
    );
    ledger_accepts(&events).finish().expect("every call ended");
}

/// A panic before or after submission ends the call's headers as panicked.
#[tokio::test]
async fn a_panicking_call_ends_its_headers() {
    for panic in [PanicAt::Method, PanicAt::Future] {
        let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort {
            panic: Some(panic),
            ..ScriptedPort::default()
        }));
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 7);
        let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);

        assert!(matches!(
            reactor.dispatch_action(prepare(&reactor, &round)),
            StorageDispatch::Started(_)
        ));
        complete_next(&mut reactor).await;

        let events = recorder.take_all();
        assert_eq!(
            ownership(&events),
            vec![(1, None), (1, Some(HeaderWorkEnd::Panicked))],
            "{panic:?}"
        );
        ledger_accepts(&events).finish().expect("every call ended");
        assert_eq!(reactor.peer_work_queue.chunk_budget_usage(), (0, 0));
    }
}

/// A reactor's teardown reports each unfinished call dropped, with its headers.
#[tokio::test]
async fn teardown_ends_each_unfinished_call_and_its_headers() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort {
        gate: Some(Arc::new(TokioSemaphore::new(0))),
        ..ScriptedPort::default()
    }));
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    assert!(matches!(
        reactor.dispatch_action(prepare(&reactor, &round)),
        StorageDispatch::Started(_)
    ));

    drop(reactor);

    let events = recorder.take_all();
    assert!(events.contains(&ReactorEvent::CallEnded {
        call: 1,
        end: CallEnd::Dropped,
    }));
    assert_eq!(
        ownership(&events),
        vec![(1, None), (1, Some(HeaderWorkEnd::Dropped))]
    );
    ledger_accepts(&events).finish().expect("every call ended");
}

/// A completion with no committed snapshot to check it against has no authority.
#[test]
fn a_prepare_without_authority_ends_as_no_authority() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort::default()));
    let peer = peer();
    let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    let target = sealed(&reactor, &peer);
    let retained = held_by_prepare(&mut reactor, &peer, 1);
    reactor.committed_snapshot = None;

    reactor.handle_target_prepared(
        peer.clone(),
        round.source,
        round.owner,
        HeaderTargetPreparationResult::Prepared(target),
        Some(retained),
    );

    assert_eq!(
        ownership(&recorder.take_all()),
        vec![(1, None), (1, Some(HeaderWorkEnd::NoAuthority))]
    );
}

/// A target sealed for another owner never reaches an apply.
#[test]
fn a_target_sealed_for_another_owner_ends_as_owner_mismatch() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort::default()));
    let peer = peer();
    let _old_outbound = connect(&mut reactor, &peer, 7);
    seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 7);
    let foreign = sealed(&reactor, &peer);
    let _new_outbound = connect(&mut reactor, &peer, 8);
    let round = seed_preparing(&mut reactor, &snapshot, &recorder, &peer, 8);
    let retained = held_by_prepare(&mut reactor, &peer, 1);

    reactor.handle_target_prepared(
        peer.clone(),
        round.source,
        round.owner,
        HeaderTargetPreparationResult::Prepared(foreign),
        Some(retained),
    );

    assert_eq!(
        ownership(&recorder.take_all()),
        vec![(1, None), (1, Some(HeaderWorkEnd::OwnerMismatch))]
    );
    assert_eq!(active(&reactor, &peer), round);
}

/// A sealed repair that doesn't match its assignment never reaches an apply.
#[test]
fn a_sealed_repair_that_misses_its_assignment_ends_as_repair_invalid() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort::default()));
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let (source, owner, _) = super::super::timeouts::seed_vct_active_request(
        &mut reactor,
        &snapshot,
        peer.clone(),
        7,
        HeaderTargetPhase::Preparing,
    );
    let target = sealed(&reactor, &peer);
    let retained = held_by_prepare(&mut reactor, &peer, 1);

    reactor.handle_target_prepared(
        peer.clone(),
        source,
        owner,
        HeaderTargetPreparationResult::Prepared(target),
        Some(retained),
    );

    assert_eq!(
        ownership(&recorder.take_all()),
        vec![(1, None), (1, Some(HeaderWorkEnd::RepairInvalid))]
    );
}

/// A refused apply ends the headers its prepare handed over, with no new call.
#[test]
fn an_apply_refused_for_a_busy_local_slot_ends_as_refused() {
    let (mut reactor, snapshot, recorder) = recorded(Arc::new(ScriptedPort::default()));
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    seed_vct_preparing(&mut reactor, &snapshot, &peer);
    let apply = apply_operation(&reactor, &peer);
    reactor.state.vct_local_operation = VctLocalOperation::from_action(&apply, Instant::now());
    let retained = held_by_prepare(&mut reactor, &peer, 1);

    let dispatch = reactor.dispatch_holding(apply, Some(retained));

    assert!(dispatch.is_refused());
    let events = recorder.take_all();
    assert!(events.contains(&ReactorEvent::StorageRefused {
        call: 1,
        kind: StorageCallKind::Apply,
        cause: observe::RefusalCause::LocalOperationBusy,
    }));
    assert_eq!(
        ownership(&events),
        vec![(1, None), (1, Some(HeaderWorkEnd::Refused))]
    );
}

fn seed_vct_preparing(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
) {
    super::super::timeouts::seed_vct_active_request(
        reactor,
        snapshot,
        peer.clone(),
        7,
        HeaderTargetPhase::Preparing,
    );
}
