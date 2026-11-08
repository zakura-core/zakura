//! The stale-completion check's rules, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, Frontier, HeaderGeneration, HeaderSyncWorkOwner,
    HeaderWorkAuthority, SourceId, VerifiedGeneration,
};
use zakura_network::zakura::observe::{
    CreditKind, HeaderRequestTerminal, HeaderTargetPhase, RequestRef, ReservationPurpose,
    StorageCallKind,
};
use zakura_network::zakura::HeaderTargetPurpose;

use super::*;

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
}

/// Ordinary work of session `session_id`.
fn owner(session_id: u64) -> HeaderSyncWorkOwner {
    let hash = block::Hash([1; 32]);
    HeaderSyncWorkOwner::Header(
        HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(1),
            branch: BranchId::new(hash, hash),
        }
        .bind(session_id, NonZeroU64::MIN),
    )
}

/// Round `incarnation` of session `session_id`.
fn round(session_id: u64, incarnation: u64) -> RequestRef {
    let owner = owner(session_id);
    RequestRef {
        peer: mirror(),
        session_id,
        request_id: incarnation,
        incarnation,
        owner,
        target_tip_hash: block::Hash([1; 32]),
    }
}

/// A body repair of session `session_id`.
fn repair_owner(session_id: u64) -> HeaderSyncWorkOwner {
    let hash = block::Hash([1; 32]);
    HeaderSyncWorkOwner::BodyRepair(
        BodyWorkAuthority {
            header: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(hash, hash),
            },
            verified_generation: VerifiedGeneration::new(1),
            body_work_epoch: BodyWorkEpoch::default(),
        }
        .bind(session_id, NonZeroU64::MIN),
    )
}

/// Session `session_id`'s prepare as call `call`, holding round
/// `incarnation`'s headers.
fn prepare(call: u64, session_id: u64, incarnation: u64) -> [ReactorEvent; 2] {
    prepare_for(call, owner(session_id), incarnation)
}

/// `owner`'s prepare as call `call`, holding round `incarnation`'s headers.
fn prepare_for(call: u64, owner: HeaderSyncWorkOwner, incarnation: u64) -> [ReactorEvent; 2] {
    [
        ReactorEvent::Dispatched {
            call,
            op: PortCall::Prepare {
                peer: mirror(),
                source: SourceId::from_digest([2; 32]),
                owner,
                target: Frontier {
                    height: block::Height(1),
                    hash: block::Hash([1; 32]),
                },
                hashes: Vec::new(),
            },
        },
        ReactorEvent::HeadersHeld {
            call,
            kind: StorageCallKind::Prepare,
            peer: mirror(),
            incarnation,
            headers: 1,
            leases: vec![incarnation],
        },
    ]
}

/// Round `request`'s retirement.
fn retired(request: RequestRef) -> ReactorEvent {
    ReactorEvent::Retired {
        request,
        phase: HeaderTargetPhase::Preparing,
        purpose: HeaderTargetPurpose::Normal,
        reason: HeaderRequestTerminal::SessionReplaced,
    }
}

/// `call`'s end, replied.
fn ended(call: u64) -> ReactorEvent {
    ReactorEvent::CallEnded {
        call,
        end: CallEnd::Replied,
    }
}

/// A `Grant` on `subscription` adding `objects` headers and `bytes`.
fn grant(
    subscription: u64,
    objects: u32,
    bytes: u32,
) -> zakura_network::zakura::v9::SubscribeHeaders {
    use zakura_network::zakura::v9::{Cursor, Operation, SubscribeHeaders};
    SubscribeHeaders {
        operation: Operation::Grant,
        subscription_id: subscription,
        update_sequence: 1,
        initial_target_tip_hash: block::Hash([1; 32]),
        acknowledged: Cursor {
            height: block::Height(0),
            hash: block::Hash([0; 32]),
            headers: 0,
            bytes: 0,
        },
        locator_hashes: Vec::new(),
        added_credit: zakura_network::zakura::Credit { objects, bytes },
        tree_aux_schema: zakura_network::zakura::AuxSchema::None,
    }
}

/// The reactor's move into `cause`.
fn cause(cause: ReactorCause) -> ReactorEvent {
    ReactorEvent::CauseSet { cause }
}

/// A budget change by `owner`.
fn changed(owner: BudgetOwner, reserved_delta: i64, owned_delta: i64) -> ReactorEvent {
    ReactorEvent::BudgetChanged {
        owner: Some(owner),
        reserved_delta,
        owned_delta,
    }
}

/// A check fed `events` on the reactor's task, and its evidence.
fn fed(events: impl IntoIterator<Item = ReactorEvent>) -> (StaleCheck, StaleEvidence) {
    let evidence = StaleEvidence::default();
    let mut check = StaleCheck::new(evidence.clone());
    for event in events {
        check.observe(&event, true);
    }
    (check, evidence)
}

/// Call 1, session 1's prepare of round 1, ends after its round retired,
/// and its handler starts.
fn retired_window() -> Vec<ReactorEvent> {
    let mut events = prepare(1, 1, 1).to_vec();
    events.extend([
        retired(round(1, 1)),
        ended(1),
        cause(ReactorCause::Completion { call: 1 }),
    ]);
    events
}

/// A call whose round retired before it ended is stale, its own returns
/// are allowed, and the drain stamps its window.
#[test]
fn a_retired_round_makes_its_call_stale_and_keeps_its_own_returns() {
    let mut events = retired_window();
    events.extend([
        changed(
            BudgetOwner::Lease {
                reservation: 4,
                round: Some((mirror(), 1)),
            },
            0,
            -1,
        ),
        ReactorEvent::RoundReleased {
            peer: mirror(),
            incarnation: 1,
            queue_owned: 0,
        },
        cause(ReactorCause::Idle),
        ReactorEvent::Handled {
            call: 1,
            panicked: false,
        },
    ]);
    let (mut check, evidence) = fed(events);
    assert_eq!(check.drain(5), Vec::<String>::new());
    assert_eq!(
        evidence.windows(),
        [StaleWindow {
            call: 1,
            apply: false,
            why: Staleness::RoundRetired,
            step: Some(5),
            handled: true,
        }]
    );
    assert!(!evidence.found_by(4));
    assert!(evidence.found_by(5));
}

/// Under a stale handler, every effect on other work breaks the claim: a
/// charge, a return of another round's lease or of an unbound reservation,
/// credit, a cursor acceptance, a built close, another round's retirement
/// or release, a start, a reservation and a prepare's dispatch. A locator
/// dispatch isn't one.
#[test]
fn a_stale_handler_breaks_on_each_effect_on_other_work() {
    let mut events = retired_window();
    let effects = [
        changed(BudgetOwner::Reservation(4), 2, 0),
        changed(
            BudgetOwner::Lease {
                reservation: 4,
                round: Some((mirror(), 2)),
            },
            0,
            -1,
        ),
        changed(BudgetOwner::Reservation(9), -1, 0),
        ReactorEvent::BudgetChanged {
            owner: None,
            reserved_delta: -1,
            owned_delta: 0,
        },
        ReactorEvent::CreditIssued {
            kind: CreditKind::Grant,
            peer: mirror(),
            session_id: 2,
            subscription: 3,
            objects: 10,
            bytes: 1_000,
            update: Box::new(grant(3, 10, 1_000)),
            target_height: block::Height(1),
        },
        ReactorEvent::CursorsAccepted {
            peer: mirror(),
            session_id: 2,
            subscription: 3,
            through: zakura_header_chain::Frontier::new(block::Height(1), block::Hash([1; 32])),
            headers: 1,
            bytes: 263,
        },
        ReactorEvent::CloseBuilt {
            peer: mirror(),
            session_id: 2,
            subscription: 3,
            update: Box::new(grant(3, 0, 0)),
        },
        retired(round(2, 2)),
        ReactorEvent::RoundReleased {
            peer: mirror(),
            incarnation: 2,
            queue_owned: 0,
        },
        ReactorEvent::Started {
            request: round(2, 3),
            purpose: HeaderTargetPurpose::Normal,
            locator: Vec::new(),
        },
        ReactorEvent::ReservationOpened {
            reservation: 5,
            peer: mirror(),
            purpose: ReservationPurpose::Ordinary,
            headers: 1,
            request: None,
        },
        prepare(2, 2, 3)[0].clone(),
    ];
    events.extend(effects.iter().cloned());
    events.push(ReactorEvent::Dispatched {
        call: 3,
        op: PortCall::Locator {
            peer: mirror(),
            session_id: 2,
            target_tip_hash: block::Hash([1; 32]),
            scope: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(block::Hash([1; 32]), block::Hash([1; 32])),
            },
        },
    });
    let (mut check, _) = fed(events);
    let breaks = check.drain(1);
    assert_eq!(breaks.len(), effects.len(), "{breaks:#?}");
    assert!(breaks
        .iter()
        .all(|detail| detail.starts_with("StaleCompletionEffect: stale prepare call 1's")));
}

/// A return joins the stale call's round through its reservation: a
/// continuation's opening names the round, a first reservation's binding
/// does once the round starts, and a lease before staging goes by its
/// reservation. Before its binding, a first reservation joins nothing.
#[test]
fn a_return_joins_its_round_through_its_reservation() {
    let mut events = vec![ReactorEvent::ReservationOpened {
        reservation: 7,
        peer: mirror(),
        purpose: ReservationPurpose::Ordinary,
        headers: 1,
        request: Some(round(1, 1)),
    }];
    events.extend(retired_window());
    let lease = |reservation| BudgetOwner::Lease {
        reservation,
        round: None,
    };
    events.extend([
        changed(BudgetOwner::Reservation(7), -1, 0),
        changed(lease(7), 0, -1),
        changed(BudgetOwner::Reservation(8), -1, 0),
        changed(lease(8), 0, -1),
        ReactorEvent::ReservationBound {
            reservation: 8,
            request: round(1, 1),
            max_headers: 1,
        },
        changed(BudgetOwner::Reservation(8), -1, 0),
        changed(lease(8), 0, -1),
    ]);
    let (mut check, _) = fed(events);
    let breaks = check.drain(1);
    assert_eq!(breaks.len(), 2, "{breaks:#?}");
    assert!(breaks
        .iter()
        .all(|detail| detail.contains("Reservation(8)") || detail.contains("reservation: 8")));
}

/// A session's end or replacement before the call's end makes it stale,
/// unless its owner is a body repair's, which only its round's retirement
/// makes stale.
#[test]
fn a_departed_session_makes_its_call_stale_unless_the_round_is_a_repair() {
    let mut events = prepare(1, 1, 1).to_vec();
    events.extend(prepare_for(2, repair_owner(1), 2));
    events.extend(prepare(3, 4, 3));
    events.extend([
        ReactorEvent::SessionAdmitted {
            peer: mirror(),
            session_id: 5,
            direction: zakura_network::zakura::ServicePeerDirection::Inbound,
            replaced: Some(1),
            origin: None,
        },
        ReactorEvent::SessionEnded {
            peer: mirror(),
            session_id: 4,
            reason: "closed",
        },
        ended(1),
        ended(2),
        ended(3),
        // A repair's retirement still makes it stale.
        retired(round(1, 4)),
    ]);
    events.extend(prepare_for(4, repair_owner(1), 4));
    events.push(ended(4));
    let (mut check, evidence) = fed(events);
    check.drain(1);
    let why: Vec<_> = evidence
        .windows()
        .iter()
        .map(|window| (window.call, window.why))
        .collect();
    assert_eq!(
        why,
        [
            (1, Staleness::SessionReplaced),
            (3, Staleness::SessionEnded),
            (4, Staleness::RoundRetired),
        ]
    );
}

/// Facts under another cause, facts from another task, and a dropped call
/// aren't judged.
#[test]
fn other_causes_tasks_and_dropped_calls_are_not_judged() {
    let mut events = retired_window();
    events.extend([
        cause(ReactorCause::Refresh { call: 1 }),
        retired(round(2, 2)),
    ]);
    let (mut check, _) = fed(events);
    check.observe(&cause(ReactorCause::Completion { call: 1 }), false);
    check.observe(&retired(round(2, 2)), false);
    assert_eq!(check.drain(1), Vec::<String>::new());

    let mut events = prepare(1, 1, 1).to_vec();
    events.extend([
        retired(round(1, 1)),
        ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::Dropped,
        },
        cause(ReactorCause::Completion { call: 1 }),
        retired(round(2, 2)),
    ]);
    let (mut check, evidence) = fed(events);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(evidence.windows().is_empty());
}

/// A stale fault's penalty isn't an effect on other work: a `Misbehavior`
/// under a stale handler breaks nothing, so a stale prepare that fails with
/// a peer fault may still penalize its peer.
#[test]
fn a_stale_faults_penalty_is_allowed() {
    let mut events = retired_window();
    events.push(ReactorEvent::Misbehavior {
        peer: mirror(),
        reason: zakura_network::zakura::HeaderSyncMisbehavior::InvalidHeader,
        origin: Some(1),
        current: Some(2),
    });
    let (mut check, _) = fed(events);
    assert_eq!(check.drain(1), Vec::<String>::new());
}
