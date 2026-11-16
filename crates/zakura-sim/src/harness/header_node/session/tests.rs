//! Each rule of the session view, fed by hand.

use super::*;

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
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

/// Only the harness's own close or a later link to the same peer makes a
/// session's end the environment's. A close the view hears of only through
/// a cancelled token, such as the node tearing its own side down, stays the
/// node's, and so does a session no link carries.
#[test]
fn only_the_harness_ends_a_session_for_the_environment() {
    let view = linked(&[1, 2]);
    view.link_closed(2);
    assert_eq!(view.ended_by(2), EndedBy::Node);
    view.harness_closed(2);
    assert_eq!(view.ended_by(2), EndedBy::Environment);
    assert_eq!(view.ended_by(1), EndedBy::Environment, "link 2 relinked it");
    assert_eq!(view.ended_by(3), EndedBy::Node, "no link carries it");
    let alone = linked(&[1]);
    alone.link_closed(1);
    assert_eq!(alone.ended_by(1), EndedBy::Node);
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
/// generation or a QUIC stream, and a second admission of one link each
/// break the join. A wrong session ID, peer or direction joins, and is
/// `header-sync/commit-only-authorized`'s claim instead.
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
    for (event, expected) in [
        (admitted(2, None, origin(1)), "its link has session 1"),
        (wrong_peer, "its link has peer mirror-0"),
        (outbound, "its link has direction Inbound"),
    ] {
        let view = linked(&[1]);
        assert_eq!(broken(&view, &[event]), None);
        let claims = view.take_claims();
        assert!(
            matches!(&claims[..], [claim]
                if claim.starts_with("CommitOnlyAuthorized:") && claim.contains(expected)),
            "{claims:?}"
        );
        assert!(view.take_joined(), "a mismatched identity still joins");
    }
    let view = linked(&[1]);
    assert_eq!(broken(&view, &[admitted(1, None, origin(1))]), None);
    assert_eq!(view.take_claims(), Vec::<String>::new());
    assert!(view.take_joined(), "a matching identity joins");
    assert!(!view.take_joined(), "the join is drained once");
}

/// Without links reported, the view keeps the facts and doesn't join.
#[test]
fn a_view_without_links_does_not_join() {
    let view = SessionEvidence::default();
    assert_eq!(broken(&view, &[admitted(1, None, None)]), None);
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
/// view stays open until own-9 retires it, and the retirement is
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
        EndCheck::Open(vec![("peer-0".to_owned(), 5)])
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
/// whose row has another kind or names no scripted peer, or whose
/// generation was installed never or twice, each break the join. So does a
/// row that only arrives after the admission. A row with another session
/// ID, or a generation that isn't the session ID, joins, and is
/// `header-sync/commit-only-authorized`'s claim instead.
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
            "names no scripted peer",
        ),
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
    ];
    for (view, event, why) in cases {
        let rule = broken(&view, &[event]).expect("the join breaks");
        assert!(rule.starts_with("Own9Join: "), "{rule}");
        assert!(rule.contains(why), "{why}: {rule}");
    }
    let claimed: Vec<(SessionEvidence, ReactorEvent, &str)> = vec![
        (
            own9_with(&|ledger| ledger.generation_installed(1, 6)),
            admitted(6, None, wire(1, 8, 6)),
            "has session 5",
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
    for (view, event, why) in claimed {
        assert_eq!(broken(&view, &[event]), None);
        let claims = view.take_claims();
        assert!(
            matches!(&claims[..], [claim]
                if claim.starts_with("CommitOnlyAuthorized: ") && claim.contains(why)),
            "{why}: {claims:?}"
        );
        assert!(view.take_joined(), "a mismatched identity still joins");
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

/// A session still unretired when the run ends stays open, though the
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
        EndCheck::Open(vec![("peer-0".to_owned(), 5)])
    );
}

/// A run that ended with `outcome` after a drain that `exhausted` or not,
/// with the scheduler at `rest` if it ran dry.
fn run_end(outcome: crate::Outcome, exhausted: bool, rest: Option<SchedulerStatus>) -> RunEnd {
    RunEnd {
        outcome,
        steps: 100,
        violation: None,
        drain: Some(crate::end::Drain {
            steps: 10,
            exhausted,
        }),
        harness: None,
        rest,
    }
}

/// A departed session own-9 never retired is a leak once the run ran dry
/// idle, live or after a drain (Terra T6), and open when the run hasn't
/// ended or stopped first, at a drain that ran dry with a timer past the
/// harness timer cap pending included.
#[test]
fn an_unretired_session_is_a_leak_only_in_an_idle_run() {
    let open = vec![("peer-0".to_owned(), 5)];
    let idle = |status, rest| Some(run_end(crate::Outcome::Idle { status }, false, Some(rest)));
    let dry = |rest| Some(run_end(crate::Outcome::Budget, false, Some(rest)));
    for (end, check) in [
        (None, EndCheck::Open(open.clone())),
        (
            Some(RunEnd {
                harness: Some(crate::HarnessFailure::DrainBound {
                    open: "task:reactor.1:poll#1".to_owned(),
                }),
                ..run_end(crate::Outcome::Harness, true, None)
            }),
            EndCheck::Open(open.clone()),
        ),
        (
            dry(SchedulerStatus::WaitingOnTime),
            EndCheck::Open(open.clone()),
        ),
        (
            dry(SchedulerStatus::WorkerStarved),
            EndCheck::Open(open.clone()),
        ),
        (
            dry(SchedulerStatus::Quiescent),
            EndCheck::Unretired(open.clone()),
        ),
        (
            dry(SchedulerStatus::WaitingOnExternal),
            EndCheck::Unretired(open.clone()),
        ),
        (
            idle(crate::IdleStatus::Deadlock, SchedulerStatus::Deadlock),
            EndCheck::Unretired(open.clone()),
        ),
        (
            idle(crate::IdleStatus::Quiescent, SchedulerStatus::Quiescent),
            EndCheck::Unretired(open.clone()),
        ),
        (
            idle(
                crate::IdleStatus::WaitingOnExternal,
                SchedulerStatus::WaitingOnExternal,
            ),
            EndCheck::Unretired(open.clone()),
        ),
    ] {
        let view = own9();
        view.reactor(&admitted(5, None, wire(1, 8, 5)));
        view.reactor(&ReactorEvent::SessionEnded {
            peer: mirror(),
            session_id: 5,
            reason: "test",
        });
        if let Some(end) = &end {
            view.end(end);
        }
        assert_eq!(view.end_check(), check, "{end:?}");
        view.ledger().retire(1, None);
        assert_eq!(view.end_check(), EndCheck::Complete, "{end:?}");
    }
}

/// The reactor's refusal of a session for its peer's cooldown.
fn refused(session_id: u64) -> ReactorEvent {
    ReactorEvent::SessionRefused {
        peer: mirror(),
        session_id,
    }
}

/// A refused session joins exactly one of the harness's links that it never
/// admitted, by peer and session ID, and is recorded as a fact.
#[test]
fn a_refused_session_joins_its_unadmitted_link() {
    let view = linked(&[1, 2]);
    assert_eq!(broken(&view, &[refused(2)]), None);
    assert!(view.facts().contains(&SessionFact::SessionRefused {
        peer: "mirror-0".to_owned(),
        session_id: 2,
    }));
    let why = broken(&linked(&[1]), &[refused(2)]).expect("no link names session 2");
    assert!(why.starts_with("SessionJoin:"), "{why}");
    let why = broken(&linked(&[1]), &[admitted(1, None, origin(1)), refused(1)])
        .expect("the link was admitted");
    assert!(why.starts_with("SessionJoin:"), "{why}");
}

/// Under own-9, a refused session joins exactly one header `Session` row by
/// peer and session ID.
#[test]
fn a_refused_session_joins_its_own9_row() {
    assert_eq!(broken(&own9(), &[refused(5)]), None);
    let why = broken(&own9(), &[refused(6)]).expect("no row has session 6");
    assert!(why.starts_with("Own9Join:"), "{why}");
}
