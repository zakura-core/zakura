//! Each rule of the session view, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::SourceId;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{HeaderRequestTerminal, HeaderTargetPhase, RequestRef},
    HeaderTargetPurpose,
};

use super::*;

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
    HeaderSyncWorkOwner::BodyRepair(
        BodyWorkAuthority {
            header: authority(),
            verified_generation: VerifiedGeneration::new(1),
            body_work_epoch: BodyWorkEpoch::default(),
        }
        .bind(session_id, NonZeroU64::MIN),
    )
}

/// The link's origin, as `Topology::link` sets it.
fn origin(id: u64) -> Option<SessionOrigin> {
    Some(SessionOrigin {
        connection: id,
        quic_stream: None,
        generation: id,
    })
}

/// The reactor's admission of the mirror's session `session_id`, inbound.
fn admitted(session_id: u64, replaced: Option<u64>, origin: Option<SessionOrigin>) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced,
        origin,
    }
}

/// The start of a request owned by `owner`.
fn started(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    ReactorEvent::Started {
        request: RequestRef {
            peer: mirror(),
            session_id: owner.session_id(),
            request_id: 1,
            incarnation: 1,
            owner,
            target_tip_hash: block::Hash([1; 32]),
        },
        purpose: HeaderTargetPurpose::Normal,
    }
}

/// A prepare (`apply` false) or apply dispatch for `owner`.
fn dispatched(apply: bool, owner: HeaderSyncWorkOwner) -> ReactorEvent {
    let (peer, source) = (mirror(), SourceId::from_digest([2; 32]));
    ReactorEvent::Dispatched {
        call: 1,
        op: if apply {
            PortCall::Apply {
                peer,
                source,
                owner,
            }
        } else {
            PortCall::Prepare {
                peer,
                source,
                owner,
            }
        },
    }
}

/// A view that joins, with the mirror's links on connections `links`, each
/// dialed by the mirror.
fn linked(links: &[u64]) -> SessionEvidence {
    let view = SessionEvidence::default();
    view.expect_links();
    for id in links {
        view.link_opened(
            &mirror(),
            "mirror-0",
            origin(*id).expect("a link has an origin"),
            *id,
            "mirror-0",
            ServicePeerDirection::Inbound,
        );
    }
    view
}

/// The rule `events` break on `view`, if any.
fn broken(view: &SessionEvidence, events: &[ReactorEvent]) -> Option<String> {
    for event in events {
        view.reactor(event);
    }
    view.broken()
}

/// An admission that agrees with its link passes, and the view keeps both
/// sides' facts in order. A close is recorded once.
#[test]
fn an_admission_joins_its_link() {
    let view = linked(&[1]);
    assert_eq!(broken(&view, &[admitted(1, None, origin(1))]), None);
    view.link_closed(1);
    view.link_closed(1);
    view.reactor(&ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 1,
        reason: "test",
    });
    assert_eq!(
        view.facts(),
        [
            SessionFact::LinkOpened {
                peer: "mirror-0".to_owned(),
                connection: 1,
                session_id: 1,
                dialer: "mirror-0".to_owned(),
            },
            SessionFact::SessionAdmitted {
                peer: "mirror-0".to_owned(),
                session_id: 1,
                direction: ServicePeerDirection::Inbound,
                replaced: None,
                origin: origin(1),
            },
            SessionFact::LinkClosed {
                peer: "mirror-0".to_owned(),
                connection: 1,
            },
            SessionFact::SessionEnded {
                peer: "mirror-0".to_owned(),
                session_id: 1,
                reason: "test".to_owned(),
            },
        ]
    );
}

/// An admission with no origin, an origin naming no link, the wrong
/// generation or a QUIC stream, a wrong session ID, peer or direction, and a
/// second admission of one link each break the join.
#[test]
fn every_join_mismatch_breaks() {
    let other = ZakuraPeerId::new(vec![3; 32]).expect("32 bytes is a valid peer ID");
    let wrong_peer = ReactorEvent::SessionAdmitted {
        peer: other,
        session_id: 1,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: origin(1),
    };
    let outbound = ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id: 1,
        direction: ServicePeerDirection::Outbound,
        replaced: None,
        origin: origin(1),
    };
    let wrong_generation = Some(SessionOrigin {
        connection: 1,
        quic_stream: None,
        generation: 2,
    });
    let with_stream = Some(SessionOrigin {
        connection: 1,
        quic_stream: Some(99),
        generation: 1,
    });
    for (events, expected) in [
        (vec![admitted(1, None, None)], "with no origin"),
        (vec![admitted(1, None, origin(9))], "names no link"),
        (
            vec![admitted(1, None, wrong_generation)],
            "its link has origin",
        ),
        (vec![admitted(1, None, with_stream)], "its link has origin"),
        (vec![admitted(2, None, origin(1))], "its link has session 1"),
        (vec![wrong_peer], "its link has peer mirror-0"),
        (vec![outbound], "its link has direction Inbound"),
        (
            vec![admitted(1, None, origin(1)), admitted(1, None, origin(1))],
            "admitted connection 1 twice",
        ),
    ] {
        let why = broken(&linked(&[1]), &events).expect("the join breaks");
        assert!(
            why.starts_with("SessionJoin:") && why.contains(expected),
            "{why}"
        );
    }
}

/// Without links reported, the view keeps the facts and doesn't join.
#[test]
fn a_view_without_links_does_not_join() {
    let view = SessionEvidence::default();
    assert_eq!(broken(&view, &[admitted(1, None, None)]), None);
}

/// After a replacement or an end, a new start or an ordinary apply naming
/// the old session breaks the rule. A prepare dispatch, work of the live
/// session, and work before the session left don't.
#[test]
fn departed_session_work_breaks() {
    let replaced = || {
        vec![
            admitted(1, None, origin(1)),
            started(ordinary(1)),
            dispatched(true, ordinary(1)),
            admitted(2, Some(1), origin(2)),
            dispatched(false, ordinary(1)),
            started(ordinary(2)),
            dispatched(true, ordinary(2)),
        ]
    };
    let ended = || {
        vec![
            admitted(1, None, origin(1)),
            ReactorEvent::SessionEnded {
                peer: mirror(),
                session_id: 1,
                reason: "test",
            },
        ]
    };
    assert_eq!(broken(&linked(&[1, 2]), &replaced()), None);
    for (mut events, late) in [
        (replaced(), started(ordinary(1))),
        (replaced(), dispatched(true, ordinary(1))),
        (ended(), started(ordinary(1))),
        (ended(), dispatched(true, ordinary(1))),
    ] {
        events.push(late);
        let why = broken(&linked(&[1, 2]), &events).expect("the late work breaks the rule");
        assert!(why.starts_with("DepartedSessionWork:"), "{why}");
    }
}

/// The retirement of `owner`'s request.
fn retired(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    let ReactorEvent::Started { request, purpose } = started(owner) else {
        unreachable!("`started` builds a start");
    };
    ReactorEvent::Retired {
        request,
        phase: HeaderTargetPhase::Applying,
        purpose,
        reason: HeaderRequestTerminal::SessionReplaced,
    }
}

/// A repair that was preparing when its session left may apply once. One
/// prepared only afterwards may not, nor one that never prepared, nor one
/// that retired or was already applying before its session left.
#[test]
fn a_kept_repair_may_apply_once() {
    let left = || ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 1,
        reason: "test",
    };
    let first = || admitted(1, None, origin(1));
    let prepare = || dispatched(false, repair(1));
    let apply = || dispatched(true, repair(1));
    for (events, legal) in [
        (vec![first(), prepare(), left(), apply()], true),
        (vec![first(), prepare(), left(), apply(), apply()], false),
        (vec![first(), left(), prepare(), apply()], false),
        (vec![first(), left(), apply()], false),
        (
            vec![first(), prepare(), retired(repair(1)), left(), apply()],
            false,
        ),
        (
            vec![first(), prepare(), left(), retired(repair(1)), apply()],
            false,
        ),
        (vec![first(), prepare(), apply(), left(), apply()], false),
    ] {
        assert_eq!(
            broken(&linked(&[1]), &events).is_none(),
            legal,
            "{events:?}"
        );
    }
}

/// The tap feeds the view every event, retirements included: a repair that
/// retires before or after its session leaves loses its apply, and one that
/// doesn't keeps it, when the events reach the view through a real tap.
#[test]
fn the_tap_feeds_retirements_to_the_view() {
    let left = || ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 1,
        reason: "test",
    };
    let first = || admitted(1, None, origin(1));
    let prepare = || dispatched(false, repair(1));
    let apply = || dispatched(true, repair(1));
    for (events, legal) in [
        (vec![first(), prepare(), left(), apply()], true),
        (
            vec![first(), prepare(), retired(repair(1)), left(), apply()],
            false,
        ),
        (
            vec![first(), prepare(), left(), retired(repair(1)), apply()],
            false,
        ),
    ] {
        let mut sim = crate::Sim::new(
            crate::harness::header_sync::storage_config(),
            crate::PlanInput::Bytes(Vec::new()),
        );
        let port = Arc::new(zakura_storage::header::controlled::ControlledPort::new(
            &zakura_storage::header::fixture::HeaderFixture::regtest(),
        ));
        let view = linked(&[1]);
        let (observer, _tap) =
            super::super::tap::ReactorTap::with_sessions(&mut sim, &port, "node", view.clone());
        for event in &events {
            observer.observe(event.clone());
        }
        let why = view.broken();
        assert_eq!(why.is_none(), legal, "{why:?} after {events:?}");
        if let Some(why) = why {
            assert!(why.starts_with("DepartedSessionWork:"), "{why}");
        }
    }
}

/// A wire origin: connection `connection`'s QUIC stream `quic_stream`, at
/// `generation`.
fn wire(connection: u64, quic_stream: u64, generation: u64) -> Option<SessionOrigin> {
    Some(SessionOrigin {
        connection,
        quic_stream: Some(quic_stream),
        generation,
    })
}

/// A view joined to own-9, where the mirror is scripted peer `peer-0` and
/// registered connection 1 from its address. Connection 1 has a header
/// `Session` row on stream 8 for session 5, and installed generation 5.
fn own9() -> SessionEvidence {
    let view = SessionEvidence::default();
    view.expect_transport(vec![(mirror(), "peer-0".to_owned())]);
    {
        let mut ledger = view.ledger();
        ledger.registered(1, crate::peer::peer_addr(0, 0));
        ledger.session_row(1, 8, ZAKURA_STREAM_HEADER_SYNC, 5);
        ledger.generation_installed(1, 5);
    }
    view
}

/// An admission that agrees with own-9 passes. Once its session ends, the
/// view is inconclusive until own-9 retires it, and the retirement is
/// recorded once.
#[test]
fn an_admission_joins_own9_and_its_end_waits_for_retirement() {
    let view = own9();
    assert_eq!(broken(&view, &[admitted(5, None, wire(1, 8, 5))]), None);
    assert_eq!(view.end_check(), EndCheck::Complete);
    view.reactor(&ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 5,
        reason: "test",
    });
    assert_eq!(
        view.end_check(),
        EndCheck::Inconclusive(vec![("peer-0".to_owned(), 5)])
    );
    view.ledger().retire(1, Some(4));
    assert_ne!(view.end_check(), EndCheck::Complete, "another generation");
    view.ledger().retire(1, Some(5));
    view.ledger().retire(1, None);
    assert_eq!(view.end_check(), EndCheck::Complete);
    let retired: Vec<_> = view
        .facts()
        .into_iter()
        .filter(|fact| matches!(fact, SessionFact::SessionRetired { .. }))
        .collect();
    assert_eq!(
        retired,
        [SessionFact::SessionRetired {
            peer: "peer-0".to_owned(),
            session_id: 5,
            by: Retirement::GenerationExited,
        }]
    );
}

/// A retirement that came first retires a session as it departs, by its
/// generation's exit or its connection's teardown, and a replacement departs
/// the session it replaced.
#[test]
fn own9_retires_a_session_in_either_order() {
    for (retire, by) in [
        (Some(5), Retirement::GenerationExited),
        (None, Retirement::TornDown),
    ] {
        let view = own9();
        view.ledger()
            .session_row(1, 12, ZAKURA_STREAM_HEADER_SYNC, 6);
        view.ledger().generation_installed(1, 6);
        assert_eq!(broken(&view, &[admitted(5, None, wire(1, 8, 5))]), None);
        view.ledger().retire(1, retire);
        assert_eq!(broken(&view, &[admitted(6, Some(5), wire(1, 12, 6))]), None);
        assert!(
            view.facts().contains(&SessionFact::SessionRetired {
                peer: "peer-0".to_owned(),
                session_id: 5,
                by,
            }),
            "{:?}",
            view.facts()
        );
        assert_eq!(view.end_check(), EndCheck::Complete);
    }
}

/// An admission with no QUIC stream, on a stream with no row or two rows,
/// whose row has another kind, peer or session ID, whose generation was
/// installed never or twice, or whose generation isn't its session ID, each
/// break the join. So does a row that only arrives after the admission.
#[test]
fn every_own9_mismatch_breaks() {
    let own9_with = |change: &dyn Fn(&mut SessionLedger)| {
        let view = own9();
        change(&mut view.ledger());
        view
    };
    let cases: Vec<(SessionEvidence, ReactorEvent, &str)> = vec![
        (own9(), admitted(5, None, None), "names no QUIC stream"),
        (own9(), admitted(5, None, origin(1)), "names no QUIC stream"),
        (own9(), admitted(5, None, wire(1, 9, 5)), "0 Session rows"),
        (own9(), admitted(5, None, wire(2, 8, 5)), "0 Session rows"),
        (
            own9_with(&|ledger| ledger.session_row(1, 8, ZAKURA_STREAM_HEADER_SYNC, 5)),
            admitted(5, None, wire(1, 8, 5)),
            "2 Session rows",
        ),
        (
            own9_with(&|ledger| {
                ledger.session_row(1, 9, 99, 5);
            }),
            admitted(5, None, wire(1, 9, 5)),
            "has kind 99",
        ),
        (
            own9_with(&|ledger| {
                ledger.registered(2, crate::peer::peer_addr(1, 0));
                ledger.session_row(2, 8, ZAKURA_STREAM_HEADER_SYNC, 5);
                ledger.generation_installed(2, 5);
            }),
            admitted(5, None, wire(2, 8, 5)),
            "has peer None",
        ),
        (own9(), admitted(6, None, wire(1, 8, 6)), "has session 5"),
        (
            own9_with(&|ledger| ledger.session_row(1, 9, ZAKURA_STREAM_HEADER_SYNC, 6)),
            admitted(6, None, wire(1, 9, 6)),
            "installed 0 times",
        ),
        (
            own9_with(&|ledger| ledger.generation_installed(1, 5)),
            admitted(5, None, wire(1, 8, 5)),
            "installed 2 times",
        ),
        (
            own9_with(&|ledger| {
                ledger.session_row(1, 9, ZAKURA_STREAM_HEADER_SYNC, 6);
                ledger.generation_installed(1, 7);
            }),
            admitted(6, None, wire(1, 9, 7)),
            "under its session ID",
        ),
    ];
    for (view, event, why) in cases {
        let rule = broken(&view, &[event]).expect("the join breaks");
        assert!(rule.starts_with("Own9Join: "), "{rule}");
        assert!(rule.contains(why), "{why}: {rule}");
    }
    let late = own9();
    let rule = broken(&late, &[admitted(6, None, wire(1, 9, 6))]).expect("no row yet");
    late.ledger()
        .session_row(1, 9, ZAKURA_STREAM_HEADER_SYNC, 6);
    late.ledger().generation_installed(1, 6);
    assert_eq!(late.broken(), Some(rule), "a later row joins nothing");
}

/// Feeds a teardown of connection 1 to its view when it drops.
struct TearDownOnDrop(SessionEvidence);

impl Drop for TearDownOnDrop {
    /// Reports connection 1's teardown, as a handler's cleanup would.
    fn drop(&mut self) {
        self.0.transport(&TransportEvent::TornDown { conn_id: 1 });
    }
}

/// A session still unretired when the run ends stays inconclusive, though the
/// run's shutdown then resumes a task whose cleanup reports its connection's
/// teardown: the view froze with the log.
#[test]
fn shutdown_cleanup_cannot_retire_a_session() {
    let mut sim = crate::Sim::new(
        crate::harness::header_sync::storage_config(),
        crate::PlanInput::Bytes(Vec::new()),
    );
    let port = Arc::new(zakura_storage::header::controlled::ControlledPort::new(
        &zakura_storage::header::fixture::HeaderFixture::regtest(),
    ));
    let view = own9();
    let (observer, tap) =
        super::super::tap::ReactorTap::with_sessions(&mut sim, &port, "node", view.clone());
    observer.observe(admitted(5, None, wire(1, 8, 5)));
    observer.observe(ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 5,
        reason: "test",
    });
    sim.add_source(Box::new(tap));
    let guard = TearDownOnDrop(view.clone());
    sim.scheduler().spawn_root("cleanup", async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    let report = sim.run(&mut crate::NoChecks, &mut crate::MemorySink::default());
    assert_eq!(report.cleanup.leaked, 0, "{report:?}");
    assert_eq!(
        view.end_check(),
        EndCheck::Inconclusive(vec![("peer-0".to_owned(), 5)])
    );
}
