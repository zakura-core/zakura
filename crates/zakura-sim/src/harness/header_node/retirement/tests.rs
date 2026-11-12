//! Each departed-work rule, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority, SourceId,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{HeaderRequestTerminal, HeaderTargetPhase, RequestRef},
    HeaderTargetPurpose, ServicePeerDirection, SessionOrigin,
};

use super::*;
use crate::harness::header_node::ends::{EndEvidence, EndFact, EnvironmentAction};
use crate::observe::{StorageFact, StorageRequest};

/// A port sequence reader for a check whose tests submit nothing after a
/// departure: every drained fact counts as published before.
fn no_port() -> PortSeq {
    Box::new(|| u64::MAX)
}

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
}

/// The authority every test owner carries.
fn authority() -> HeaderWorkAuthority {
    let hash = block::Hash([1; 32]);
    HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(1),
        branch: BranchId::new(hash, hash),
    }
}

/// Ordinary work of session `session_id`.
fn ordinary(session_id: u64) -> HeaderSyncWorkOwner {
    HeaderSyncWorkOwner::Header(authority().bind(session_id, NonZeroU64::MIN))
}

/// An auxiliary repair of session `session_id`.
fn repair(session_id: u64) -> HeaderSyncWorkOwner {
    repair_request(session_id, NonZeroU64::MIN)
}

/// An auxiliary repair of session `session_id`, for its request `request`.
fn repair_request(session_id: u64, request: NonZeroU64) -> HeaderSyncWorkOwner {
    HeaderSyncWorkOwner::BodyRepair(
        BodyWorkAuthority {
            header: authority(),
            verified_generation: VerifiedGeneration::new(1),
            body_work_epoch: BodyWorkEpoch::default(),
        }
        .bind(session_id, request),
    )
}

/// The reactor's admission of the mirror's session `session_id`.
fn admitted(session_id: u64, replaced: Option<u64>) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced,
        origin: None,
    }
}

/// The end of the mirror's session `session_id`.
fn left(session_id: u64) -> ReactorEvent {
    ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id,
        reason: "test",
    }
}

/// The request identity of `owner`'s round `incarnation`.
fn request(owner: HeaderSyncWorkOwner, incarnation: u64) -> RequestRef {
    RequestRef {
        peer: mirror(),
        session_id: owner.session_id(),
        request_id: 1,
        incarnation,
        owner,
        target_tip_hash: block::Hash([1; 32]),
    }
}

/// The start of round 1 of a request owned by `owner`.
fn started(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    started_round(owner, 1)
}

/// The start of round `incarnation` of a request owned by `owner`.
fn started_round(owner: HeaderSyncWorkOwner, incarnation: u64) -> ReactorEvent {
    ReactorEvent::Started {
        request: request(owner, incarnation),
        purpose: HeaderTargetPurpose::Normal,
        locator: Vec::new(),
    }
}

/// The retirement of round 1 of `owner`'s request.
fn retired(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    ReactorEvent::Retired {
        request: request(owner, 1),
        phase: HeaderTargetPhase::Applying,
        purpose: HeaderTargetPurpose::Normal,
        reason: HeaderRequestTerminal::SessionReplaced,
    }
}

/// Call `call`'s prepare (`apply` false) or apply dispatch for `owner`.
fn dispatched(call: u64, apply: bool, owner: HeaderSyncWorkOwner) -> ReactorEvent {
    let (peer, source) = (mirror(), SourceId::from_digest([2; 32]));
    ReactorEvent::Dispatched {
        call,
        op: if apply {
            PortCall::Apply {
                peer,
                source,
                owner,
                target_tip_hash: block::Hash([1; 32]),
            }
        } else {
            PortCall::Prepare {
                peer,
                source,
                owner,
                common_ancestor: zakura_header_chain::Frontier::new(
                    block::Height(0),
                    block::Hash([0; 32]),
                ),
                target: zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0; 32])),
                hashes: Vec::new(),
            }
        },
    }
}

/// The reactor's handling of call `call`.
fn handled(call: u64) -> ReactorEvent {
    ReactorEvent::Handled {
        call,
        panicked: false,
    }
}

/// The reactor entering `cause`.
fn cause(cause: ReactorCause) -> ReactorEvent {
    ReactorEvent::CauseSet { cause }
}

/// The handler of call `call`'s completion.
fn completion(call: u64) -> ReactorEvent {
    cause(ReactorCause::Completion { call })
}

/// The run loop between units of work.
fn idle() -> ReactorEvent {
    cause(ReactorCause::Idle)
}

/// The queue listing one slot of `owner`'s session, a repair's or not, in
/// `phase`, as round 1 once it has a phase.
fn slot(owner: &HeaderSyncWorkOwner, phase: Option<HeaderTargetPhase>) -> ReactorEvent {
    slot_round(owner, phase, u64::from(phase.is_some()))
}

/// The queue listing one slot of `owner`'s session in `phase`, as round
/// `incarnation`.
fn slot_round(
    owner: &HeaderSyncWorkOwner,
    phase: Option<HeaderTargetPhase>,
    incarnation: u64,
) -> ReactorEvent {
    ReactorEvent::Slots {
        slots: vec![WorkSlot {
            peer: mirror(),
            session_id: owner.session_id(),
            target_tip_hash: block::Hash([1; 32]),
            phase,
            incarnation,
            repair: matches!(owner, HeaderSyncWorkOwner::BodyRepair(_)),
            authority: authority(),
        }],
    }
}

/// The queue listing no slot.
fn no_slots() -> ReactorEvent {
    ReactorEvent::Slots { slots: Vec::new() }
}

/// The breaks `events` give, fed in order from the reactor, none inside a
/// stale handler, and the evidence.
fn breaks(events: &[ReactorEvent]) -> (Vec<String>, RetirementEvidence) {
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in events {
        check.observe(event, true, false);
    }
    (check.drain(1), evidence)
}

/// After a replacement or an end, a new start or an ordinary apply naming
/// the old session breaks the rule. A prepare dispatch, work of the live
/// session, and work before the session left don't.
#[test]
fn departed_session_work_breaks() {
    let replaced = || {
        vec![
            admitted(1, None),
            started(ordinary(1)),
            dispatched(1, true, ordinary(1)),
            admitted(2, Some(1)),
            dispatched(2, false, ordinary(1)),
            started(ordinary(2)),
            dispatched(3, true, ordinary(2)),
        ]
    };
    let ended = || vec![admitted(1, None), left(1)];
    assert_eq!(breaks(&replaced()).0, Vec::<String>::new());
    for (mut events, late) in [
        (replaced(), started(ordinary(1))),
        (replaced(), dispatched(4, true, ordinary(1))),
        (ended(), started(ordinary(1))),
        (ended(), dispatched(4, true, ordinary(1))),
    ] {
        events.push(late);
        let (found, _) = breaks(&events);
        assert_eq!(found.len(), 1, "{events:?}: {found:?}");
        assert!(found[0].starts_with("PeerRetirementProgress:"), "{found:?}");
    }
}

/// Late work for a departed session inside a stale call's own handler is
/// `header-sync/stale-completion-inert`'s, so it breaks nothing here.
#[test]
fn work_in_a_stale_handler_is_not_judged() {
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence,
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in [admitted(1, None), left(1)] {
        check.observe(&event, true, false);
    }
    check.observe(&started(ordinary(1)), true, true);
    check.observe(&dispatched(4, true, ordinary(1)), true, true);
    assert_eq!(check.drain(1), Vec::<String>::new());
}

/// A repair that was preparing when its session left may apply once, from
/// its prepare's handler. One applying again, one prepared only afterwards,
/// one that never prepared, one whose apply came under another cause, and
/// one that retired before applying may not.
#[test]
fn a_kept_repair_may_apply_once_from_its_prepare() {
    let prepare = || dispatched(1, false, repair(1));
    let apply = |call| dispatched(call, true, repair(1));
    // The repair's round 1 starts and holds the session's slot while it
    // prepares.
    let first = || -> Vec<ReactorEvent> {
        vec![
            admitted(1, None),
            started(repair(1)),
            slot(&repair(1), Some(HeaderTargetPhase::Preparing)),
        ]
    };
    let with = |tail: Vec<ReactorEvent>| -> Vec<ReactorEvent> {
        let mut events = first();
        events.extend(tail);
        events
    };
    for (events, legal) in [
        (
            with(vec![prepare(), left(1), completion(1), apply(2)]),
            true,
        ),
        (
            with(vec![
                prepare(),
                left(1),
                completion(1),
                apply(2),
                completion(1),
                apply(3),
            ]),
            false,
        ),
        (
            with(vec![left(1), prepare(), completion(1), apply(2)]),
            false,
        ),
        (with(vec![left(1), completion(1), apply(2)]), false),
        (with(vec![prepare(), left(1), idle(), apply(2)]), false),
        (
            with(vec![
                prepare(),
                left(1),
                retired(repair(1)),
                completion(1),
                apply(2),
            ]),
            false,
        ),
        (
            with(vec![
                prepare(),
                completion(1),
                apply(2),
                left(1),
                completion(2),
                apply(3),
            ]),
            false,
        ),
    ] {
        assert_eq!(breaks(&events).0.is_empty(), legal, "{events:?}");
    }
}

/// A slot that names a departed session breaks at the next unit boundary,
/// once, however many boundaries pass. A slot of a live session doesn't,
/// nor one the departure's own unit removed.
#[test]
fn a_departed_slot_breaks_at_the_next_boundary() {
    let owner = ordinary(1);
    let awaiting = slot(&owner, None);
    let active = slot(&owner, Some(HeaderTargetPhase::Receiving));
    assert!(
        breaks(&[admitted(1, None), awaiting.clone(), idle(), idle()])
            .0
            .is_empty()
    );
    let (found, evidence) = breaks(&[
        admitted(1, None),
        active.clone(),
        idle(),
        cause(ReactorCause::Event),
        no_slots(),
        left(1),
        idle(),
    ]);
    assert_eq!(found, Vec::<String>::new());
    assert!(evidence.applicable(), "a session departed with a slot");
    for kept in [awaiting, active] {
        let (found, _) = breaks(&[
            admitted(1, None),
            kept.clone(),
            idle(),
            cause(ReactorCause::Event),
            admitted(2, Some(1)),
            idle(),
            cause(ReactorCause::Maintenance),
            idle(),
        ]);
        assert_eq!(found.len(), 1, "{kept:?}: {found:?}");
        assert!(found[0].contains("outlived"), "{found:?}");
    }
}

/// A kept repair's slot is excused until its in-flight call is handled,
/// then until the apply its prepare's handler dispatched is handled. At the
/// first boundary after that, it must be gone. An ordinary slot of the same
/// session gets no excuse.
#[test]
fn a_kept_repair_slot_is_excused_until_its_calls_are_handled() {
    let owner = repair(1);
    let preparing = slot(&owner, Some(HeaderTargetPhase::Preparing));
    let applying = slot(&owner, Some(HeaderTargetPhase::Applying));
    let kept = || {
        vec![
            admitted(1, None),
            started(owner),
            dispatched(1, false, owner),
            preparing.clone(),
            idle(),
            cause(ReactorCause::Event),
            left(1),
            idle(),
            completion(1),
            dispatched(2, true, owner),
            applying.clone(),
            idle(),
            cause(ReactorCause::Refresh { call: 1 }),
            idle(),
            handled(1),
            completion(2),
        ]
    };
    let mut gone = kept();
    gone.extend([no_slots(), idle(), handled(2), idle()]);
    assert_eq!(breaks(&gone).0, Vec::<String>::new());
    let mut stays = kept();
    stays.extend([idle(), handled(2), cause(ReactorCause::Maintenance)]);
    let (found, _) = breaks(&stays);
    assert_eq!(found.len(), 1, "{found:?}");
    let ordinary_slot = slot(&ordinary(1), Some(HeaderTargetPhase::Preparing));
    let (found, _) = breaks(&[
        admitted(1, None),
        dispatched(1, false, owner),
        ordinary_slot,
        idle(),
        cause(ReactorCause::Event),
        left(1),
        idle(),
    ]);
    assert_eq!(found.len(), 1, "an ordinary slot gets no excuse: {found:?}");
}

/// Facts from another task than the reactor's are ignored.
#[test]
fn facts_off_the_reactor_are_ignored() {
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in [admitted(1, None), left(1), started(ordinary(1))] {
        check.observe(&event, false, false);
    }
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(!evidence.applicable());
}

/// A retired repair's call still in flight excuses no other round: once
/// repair round 1 retires with its prepare held, and the session's newer
/// repair round 2 receives, a departure that leaves round 2's slot breaks
/// at the next boundary, and its apply would too.
#[test]
fn a_retired_repairs_call_excuses_no_newer_round() {
    let first = repair(1);
    let second = repair_request(1, NonZeroU64::new(2).expect("2 is nonzero"));
    let (found, _) = breaks(&[
        admitted(1, None),
        started_round(first, 1),
        dispatched(1, false, first),
        slot_round(&first, Some(HeaderTargetPhase::Preparing), 1),
        retired(first),
        started_round(second, 2),
        slot_round(&second, Some(HeaderTargetPhase::Receiving), 2),
        idle(),
        cause(ReactorCause::Event),
        left(1),
        idle(),
        completion(1),
        dispatched(2, true, second),
    ]);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].contains("outlived"), "{found:?}");
    assert!(found[1].contains("applies"), "{found:?}");
}

/// Another mirror's peer ID.
fn other() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![3; 32]).expect("32 bytes is a valid peer ID")
}

/// The admission of the other mirror's session `session_id`.
fn other_admitted(session_id: u64) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer: other(),
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: None,
    }
}

/// The admission of `peer`'s session `session_id` on `connection`.
fn admitted_on(peer: ZakuraPeerId, session_id: u64, connection: u64) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer,
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: Some(SessionOrigin {
            connection,
            quic_stream: None,
            generation: connection,
        }),
    }
}

/// A hand-off check armed for `mirror-0`, the mirror, as the owner and
/// `mirror-1`, the other mirror, as the taker, with a free locator permit:
/// the owner linked on connection 1, the taker on 2 and relinked on 3. Its
/// session and end evidence come back too.
fn armed() -> (Retirement, RetirementEvidence, EndEvidence) {
    let evidence = RetirementEvidence::armed(
        &EvidenceStores::default(),
        Roles {
            owner: "mirror-0".to_owned(),
            taker: "mirror-1".to_owned(),
        },
    );
    let (sessions, ends) = (
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
    );
    for (peer, name, connection) in [
        (mirror(), "mirror-0", 1),
        (other(), "mirror-1", 2),
        (other(), "mirror-1", 3),
    ] {
        let origin = SessionOrigin {
            connection,
            quic_stream: None,
            generation: connection,
        };
        sessions.link_opened(
            &peer,
            name,
            origin,
            connection,
            name,
            ServicePeerDirection::Inbound,
        );
    }
    ends.record(EndFact::Environment {
        peer: "mirror-1".to_owned(),
        connection: Some(3),
        action: EnvironmentAction::Relink,
    });
    let mut check = Retirement::new(evidence.clone(), sessions, ends.clone(), no_port());
    check.permits(16);
    (check, evidence, ends)
}

/// Events up to the fixture's readiness: the owner's session 1 holds an
/// awaiting slot, and the taker's relinked session 3 is admitted.
fn readied() -> Vec<ReactorEvent> {
    vec![
        admitted_on(mirror(), 1, 1),
        slot(&ordinary(1), None),
        idle(),
        admitted_on(other(), 2, 2),
        cause(ReactorCause::Event),
        admitted_on(other(), 3, 3),
        idle(),
    ]
}

/// The hand-off facts `events` give an armed check, fed in order from the
/// reactor, with `before` run on the check and its end evidence after
/// readiness.
fn hand_offs(
    events: &[ReactorEvent],
    before: impl FnOnce(&mut Retirement, &EndEvidence),
) -> (Vec<HandoffFact>, RetirementEvidence) {
    let (mut check, evidence, ends) = armed();
    for event in readied() {
        check.observe(&event, true, false);
    }
    before(&mut check, &ends);
    for event in events {
        check.observe(event, true, false);
    }
    let facts = evidence.facts().into_iter().map(|(_, fact)| fact).collect();
    (facts, evidence)
}

/// The owner's slot going, and the boundary after.
fn owner_slot_goes() -> Vec<ReactorEvent> {
    vec![cause(ReactorCause::Event), no_slots(), left(1), idle()]
}

/// The fixture is ready only at the taker's admission on the relink the
/// harness made, naming the owner's live session, and only while that
/// session holds a slot. The taker's first admission readies nothing.
#[test]
fn the_fixture_is_ready_at_the_takers_relinked_admission() {
    let ready = |events: &[ReactorEvent]| -> Vec<HandoffFact> {
        let (mut check, evidence, _) = armed();
        for event in events {
            check.observe(event, true, false);
        }
        evidence
            .facts()
            .into_iter()
            .map(|(_, fact)| fact)
            .filter(|fact| matches!(fact, HandoffFact::Ready { .. }))
            .collect()
    };
    assert_eq!(
        ready(&readied()),
        [HandoffFact::Ready {
            owner: (mirror(), 1),
            taker: (other(), 3),
        }]
    );
    assert_eq!(ready(&readied()[..4]), [], "the first admission");
    let without_slot = [
        admitted_on(mirror(), 1, 1),
        idle(),
        cause(ReactorCause::Event),
        admitted_on(other(), 3, 3),
    ];
    assert_eq!(ready(&without_slot), [], "an owner with no slot");
}

/// Once ready, the owner's slot going opens one hand-off. A slot that went
/// before readiness opens nothing, nor one that goes once the harness began
/// stopping the node, which also leaves that departure inapplicable.
#[test]
fn a_hand_off_opens_only_once_ready_and_before_shutdown() {
    let opened = |facts: &[HandoffFact]| {
        facts
            .iter()
            .filter(|fact| matches!(fact, HandoffFact::Opened { .. }))
            .count()
    };
    let (facts, _) = hand_offs(&owner_slot_goes(), |_, _| {});
    assert_eq!(opened(&facts), 1, "{facts:?}");
    let (mut check, evidence, _) = armed();
    let early = [
        admitted_on(mirror(), 1, 1),
        slot(&ordinary(1), None),
        idle(),
        cause(ReactorCause::Event),
        no_slots(),
        idle(),
        cause(ReactorCause::Event),
        admitted_on(other(), 3, 3),
        idle(),
    ];
    for event in early {
        check.observe(&event, true, false);
    }
    assert_eq!(
        opened(
            &evidence
                .facts()
                .into_iter()
                .map(|(_, f)| f)
                .collect::<Vec<_>>()
        ),
        0
    );
    let (facts, evidence) = hand_offs(&owner_slot_goes(), |_, ends| {
        ends.record(EndFact::Environment {
            peer: "node".to_owned(),
            connection: None,
            action: EnvironmentAction::Shutdown,
        });
    });
    assert_eq!(opened(&facts), 0, "{facts:?}");
    assert!(!evidence.opened_before(None));
}

/// The fixture's conditions at the trigger (Rule 3): a taker the harness
/// ended since its relink, one holding a slot, one that already read the
/// target, or no free permit opens nothing. A permit no probe read gave
/// opens nothing either, but marks the evidence unread.
#[test]
fn a_hand_off_opens_only_where_the_fixture_held() {
    let opens = |before: &dyn Fn(&mut Retirement, &EndEvidence), events: &[ReactorEvent]| {
        let (facts, evidence) = hand_offs(events, before);
        (
            facts
                .iter()
                .any(|fact| matches!(fact, HandoffFact::Opened { .. })),
            evidence.unread(),
        )
    };
    let goes = owner_slot_goes();
    assert_eq!(opens(&|_, _| {}, &goes), (true, false));
    let ended = |_: &mut Retirement, ends: &EndEvidence| {
        ends.record(EndFact::Environment {
            peer: "mirror-1".to_owned(),
            connection: Some(3),
            action: EnvironmentAction::Close,
        });
    };
    assert_eq!(opens(&ended, &goes), (false, false), "taker ended");
    let mut taker_slot = vec![ReactorEvent::Slots {
        slots: vec![WorkSlot {
            peer: other(),
            session_id: 3,
            target_tip_hash: block::Hash([1; 32]),
            phase: None,
            incarnation: 0,
            repair: false,
            authority: authority(),
        }],
    }];
    taker_slot.splice(0..0, [cause(ReactorCause::Event)]);
    taker_slot.extend([left(1), idle()]);
    assert_eq!(opens(&|_, _| {}, &taker_slot), (false, false), "taker slot");
    let mut read = vec![locator(9, other(), 3)];
    read.extend(owner_slot_goes());
    assert_eq!(opens(&|_, _| {}, &read), (false, false), "taker read");
    assert_eq!(
        opens(&|check, _| check.permits(0), &goes),
        (false, false),
        "no permit"
    );
    assert_eq!(
        opens(&|check, _| check.permits = None, &goes),
        (false, true),
        "no probe read"
    );
}

/// Only the owner's round records its arms, and only once the fixture is
/// ready. Each arm obliges only where the fixture held.
#[test]
fn only_the_owners_arms_once_ready_record() {
    let arms = |events: &[ReactorEvent], before: &dyn Fn(&mut Retirement, &EndEvidence)| {
        let (facts, _) = hand_offs(events, before);
        facts
            .into_iter()
            .filter_map(|fact| match fact {
                HandoffFact::Armed {
                    session, obliges, ..
                } => Some((session.1, obliges)),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let taker_round = HeaderSyncWorkOwner::Header(authority().bind(3, NonZeroU64::MIN));
    let taker_started = ReactorEvent::Started {
        request: RequestRef {
            peer: other(),
            ..request(taker_round, 1)
        },
        purpose: HeaderTargetPurpose::Normal,
        locator: Vec::new(),
    };
    assert_eq!(
        arms(&[started(ordinary(1)), taker_started], &|_, _| {}),
        [(1, true)]
    );
    assert_eq!(
        arms(&[started(ordinary(1))], &|check, _| check.permits(0)),
        [(1, false)]
    );
    let (mut check, evidence, _) = armed();
    for event in [admitted_on(mirror(), 1, 1), started(ordinary(1))] {
        check.observe(&event, true, false);
    }
    assert!(evidence.facts().is_empty(), "an arm before readiness");
}

/// A departure the slot rule hasn't judged at a unit boundary leaves the
/// evidence unjudged, until the next boundary.
#[test]
fn a_departure_is_unjudged_until_the_next_boundary() {
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in [
        admitted(1, None),
        idle(),
        cause(ReactorCause::Event),
        left(1),
    ] {
        check.observe(&event, true, false);
    }
    assert!(evidence.unjudged());
    check.observe(&idle(), true, false);
    assert!(!evidence.unjudged());
}

/// A locator read the node dispatched for the target.
fn locator(call: u64, peer: ZakuraPeerId, session_id: u64) -> ReactorEvent {
    ReactorEvent::Dispatched {
        call,
        op: PortCall::Locator {
            peer,
            session_id,
            target_tip_hash: block::Hash([1; 32]),
            scope: authority(),
        },
    }
}

/// Call `call`'s operation reaching storage.
fn submitted(call: u64) -> StorageFact {
    StorageFact::Submitted {
        id: call,
        request: StorageRequest::ContinuationLocator,
        digest: String::new(),
        call: Some(call),
    }
}

/// A locator read reaching storage answers a hand-off only if the node's
/// port published its submission before the read's session departed, by
/// the port's own sequence numbers, however late the tap drains it: a read
/// a departed session's call submits afterwards counts for nothing, even
/// within the departure's own poll.
#[test]
fn a_departed_sessions_read_counts_for_nothing() {
    let (_, evidence, ends) = armed();
    let published = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let seq = published.clone();
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        ends,
        Box::new(move || seq.load(std::sync::atomic::Ordering::SeqCst)),
    );
    check.permits(16);
    for event in [
        other_admitted(2),
        locator(1, other(), 2),
        locator(2, other(), 2),
    ] {
        check.observe(&event, true, false);
    }
    // Call 1 submits as port fact 1, then the session departs, then call 2
    // submits as fact 2, all before the tap drains either.
    published.store(1, std::sync::atomic::Ordering::SeqCst);
    check.observe(
        &ReactorEvent::SessionEnded {
            peer: other(),
            session_id: 2,
            reason: "test",
        },
        true,
        false,
    );
    check.storage(1, &submitted(1));
    check.storage(2, &submitted(2));
    let reads: Vec<_> = evidence
        .facts()
        .into_iter()
        .filter(|(_, fact)| matches!(fact, HandoffFact::Read { .. }))
        .collect();
    assert_eq!(
        reads.len(),
        1,
        "only the read before the departure: {reads:?}"
    );
}

/// Each of the three events production arms a round's deadline with
/// records the next arm of the owner's round: its start as arm 0, then each
/// staged page and its apply's dispatch. A later arm of the round answers an
/// earlier one, which `header-sync/peer-retirement-progress` reads.
#[test]
fn every_arm_event_records_the_next_arm() {
    let staged = ReactorEvent::PageStaged {
        from_peer: mirror(),
        from_session: 1,
        request: request(ordinary(1), 1),
        reservation: 1,
        subscription: 1,
        frame: None,
        headers: 0,
        reserved_left: 0,
        ancestor: zakura_header_chain::Frontier::new(block::Height(0), block::Hash([0; 32])),
        hashes: Vec::new(),
        stored: Vec::new(),
        staged_total: 0,
    };
    let (facts, _) = hand_offs(
        &[
            started(ordinary(1)),
            staged.clone(),
            staged,
            dispatched(5, true, ordinary(1)),
        ],
        |_, _| {},
    );
    let arms: Vec<(u64, u32, bool)> = facts
        .into_iter()
        .filter_map(|fact| match fact {
            HandoffFact::Armed {
                round,
                arm,
                obliges,
                ..
            } => Some((round, arm, obliges)),
            _ => None,
        })
        .collect();
    assert_eq!(
        arms,
        [(1, 0, true), (1, 1, true), (1, 2, true), (1, 3, true)]
    );
}

/// A run that ends with `outcome`, after a drain that ran out of ready
/// tasks (`exhausted` false) or stopped at its bound, if `drain` says.
fn run_end(outcome: crate::Outcome, drain: Option<bool>) -> crate::RunEnd {
    crate::RunEnd {
        outcome,
        steps: 100,
        violation: None,
        drain: drain.map(|exhausted| crate::end::Drain {
            steps: 10,
            exhausted,
        }),
        harness: None,
        // A dry end is quiescent unless it is an idle one that says
        // otherwise.
        rest: match outcome {
            crate::Outcome::Idle { status } => Some(match status {
                crate::IdleStatus::Quiescent => zakura_scheduler::Status::Quiescent,
                crate::IdleStatus::Deadlock => zakura_scheduler::Status::Deadlock,
                crate::IdleStatus::WorkerStarved => zakura_scheduler::Status::WorkerStarved,
                crate::IdleStatus::WaitingOnExternal => zakura_scheduler::Status::WaitingOnExternal,
            }),
            crate::Outcome::Budget if drain == Some(false) => {
                Some(zakura_scheduler::Status::Quiescent)
            }
            _ => None,
        },
    }
}

/// A departure whose unit never reached its boundary is judged when the
/// run settles, as `adoption::settled` decides: a slot left with the
/// departed session breaks at a quiescent or deadlocked end, or after a
/// budget drain that ran dry. A worker-starved end, which can stop a task
/// inside a unit, or a budget end whose drain hit its bound, leaves it
/// unjudged.
#[test]
fn a_settled_run_judges_an_unfinished_departure() {
    let left_with_slot = |end| {
        let evidence = RetirementEvidence::new(&EvidenceStores::default());
        let mut check = Retirement::new(
            evidence.clone(),
            SessionEvidence::new(&EvidenceStores::default()),
            EndEvidence::new(&EvidenceStores::default()),
            no_port(),
        );
        for event in [
            admitted(1, None),
            slot(&ordinary(1), Some(HeaderTargetPhase::Receiving)),
            idle(),
            cause(ReactorCause::Event),
            left(1),
        ] {
            check.observe(&event, true, false);
        }
        evidence.end(&end);
        evidence.settled_breaks().len()
    };
    let idle = |status| crate::Outcome::Idle { status };
    for (end, judged) in [
        (run_end(idle(crate::IdleStatus::Quiescent), None), 1),
        (run_end(idle(crate::IdleStatus::WaitingOnExternal), None), 1),
        (run_end(crate::Outcome::Budget, Some(false)), 1),
        (run_end(idle(crate::IdleStatus::Deadlock), None), 1),
        (run_end(idle(crate::IdleStatus::WorkerStarved), None), 0),
        (run_end(crate::Outcome::Budget, Some(true)), 0),
        (run_end(crate::Outcome::Budget, None), 0),
    ] {
        assert_eq!(left_with_slot(end.clone()), judged, "{end:?}");
    }
}

/// A kept repair's excuse that ends after its departure's boundary leaves
/// the slot rule unjudged until the next one, and a run that settles first
/// is judged on the slot it ended with.
#[test]
fn an_expired_excuse_is_judged_at_a_settled_end() {
    let owner = repair(1);
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in [
        admitted(1, None),
        started(owner),
        dispatched(1, false, owner),
        slot(&owner, Some(HeaderTargetPhase::Preparing)),
        idle(),
        cause(ReactorCause::Event),
        left(1),
        idle(),
        cause(ReactorCause::Refresh { call: 1 }),
    ] {
        check.observe(&event, true, false);
    }
    assert!(!evidence.unjudged(), "the departure's boundary judged it");
    check.observe(&handled(1), true, false);
    assert!(evidence.unjudged(), "the excuse ended after that boundary");
    evidence.end(&run_end(
        crate::Outcome::Idle {
            status: crate::IdleStatus::Quiescent,
        },
        None,
    ));
    let settled = evidence.settled_breaks();
    assert_eq!(settled.len(), 1, "{settled:?}");
    assert!(settled[0].contains("outlived"), "{settled:?}");
}

/// A kept repair's slot is excused at a settled end right after its
/// session departs, before any later boundary or `Slots` listing.
#[test]
fn a_kept_repair_is_excused_at_a_settled_end() {
    let owner = repair(1);
    let evidence = RetirementEvidence::new(&EvidenceStores::default());
    let mut check = Retirement::new(
        evidence.clone(),
        SessionEvidence::new(&EvidenceStores::default()),
        EndEvidence::new(&EvidenceStores::default()),
        no_port(),
    );
    for event in [
        admitted(1, None),
        started(owner),
        dispatched(1, false, owner),
        slot(&owner, Some(HeaderTargetPhase::Preparing)),
        idle(),
        cause(ReactorCause::Event),
        left(1),
    ] {
        check.observe(&event, true, false);
    }
    evidence.end(&run_end(
        crate::Outcome::Idle {
            status: crate::IdleStatus::Quiescent,
        },
        None,
    ));
    assert_eq!(evidence.settled_breaks(), Vec::<String>::new());
}

/// The owner's own replacement session may take the target up first, as
/// the plan allows: its new `Status` can reach the reactor before the
/// cached pass offers the target to the taker. Its locator read for the
/// target discharges the hand-off the departed session's slot opened. The
/// liveness fixtures can't produce this order, since the replacement's own
/// turn hands the target to the taker, so it's checked here at the
/// property's answer.
#[test]
fn the_owners_replacement_answers_a_hand_off() {
    let (mut check, evidence, ends) = armed();
    for event in readied() {
        check.observe(&event, true, false);
    }
    for event in [
        cause(ReactorCause::Event),
        ReactorEvent::SessionAdmitted {
            peer: mirror(),
            session_id: 4,
            direction: ServicePeerDirection::Inbound,
            replaced: Some(1),
            origin: Some(SessionOrigin {
                connection: 4,
                quic_stream: None,
                generation: 4,
            }),
        },
        no_slots(),
        idle(),
        locator(9, mirror(), 4),
    ] {
        check.observe(&event, true, false);
    }
    check.storage(1, &submitted(9));
    let facts = evidence.facts();
    let (opened_at, opener) = facts
        .iter()
        .find(|(_, fact)| matches!(fact, HandoffFact::Opened { .. }))
        .cloned()
        .expect("the departed session's slot going opens a hand-off");
    let read_at = facts
        .iter()
        .find_map(|(stamp, fact)| {
            matches!(fact, HandoffFact::Read { session, .. } if *session == (mirror(), 4))
                .then_some(*stamp)
        })
        .expect("the replacement's read reaches storage");
    assert!(read_at > opened_at);
    assert!(matches!(
        crate::domain::header_sync::retirement_answer(&opener, read_at, &evidence, &ends),
        crate::property::FactAnswer::Discharges
    ));
}
