//! hf-7: the topology's session lifetimes ([`Effects::ended`]), the one
//! part of a header harness's view that's a copy, not a store handle.
//! Each link's recorded end is the instant its session's token was
//! cancelled, and `header-sync/storage`'s next view shows a link the
//! harness closed, with no reactor event between.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};

use support::{inputs, one_key_off::assert_one_key_off, run};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::after_step,
    harness::{
        header_acceptance::refs::{ConnectionRef, Ref, SessionIdRef},
        header_node::{
            adoption::SessionLife, policy::Trigger, preload, tap::ReactorTap, Chain, Control,
            ControlAction, ControlStep, DispatchFlag, Effects, HeaderNode, Policy, PolicySource,
            Topology,
        },
        header_sync::{storage_config, Storage, StorageScenario},
        Subsystem,
    },
    property::HasView,
    stores::EvidenceStores,
    Category, Identity, Observation,
};
use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

/// `at` as virtual time since the run's start.
fn since_start(at: SimInstant) -> Duration {
    at.saturating_duration_since(SimInstant::from_start(Duration::ZERO))
}

/// A `peer` step doing `action` once `when` holds.
fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// Where one session's end first showed in the storage harness's view.
#[derive(Debug)]
struct Shown {
    /// The session.
    session_id: u64,
    /// The end the view showed.
    until: Duration,
    /// The pick of the step after which it first showed.
    pick: Identity,
    /// Virtual time after that step.
    now: Duration,
    /// Whether that step logged a reactor event.
    reactor_event: bool,
    /// How many sessions that view held.
    sessions: usize,
}

/// In the close-and-relink scenario, the harness closes each mirror's
/// first link from its control task, and the storage view after that very
/// step shows the session ended at that step's time, before the mirror's
/// relink. Nothing else moved in between: the step logged no reactor
/// event, so the view read the topology's lifetimes when called, not a
/// copy a reactor event refreshed.
#[test]
fn a_link_the_harness_closes_shows_in_the_next_view() {
    let scenario = StorageScenario::close_relink();
    for input in inputs(8, 256) {
        let storage: Rc<RefCell<Option<Rc<Storage>>>> = Rc::default();
        let previous: RefCell<Vec<SessionLife>> = RefCell::default();
        let shown: RefCell<Vec<Shown>> = RefCell::default();
        let mut checker = after_step(|cx| {
            let view = storage
                .borrow()
                .as_ref()
                .expect("the setup built the harness")
                .view();
            let before = previous.replace(view.sessions.clone());
            for life in &view.sessions {
                if let (None, Some(until)) = (earlier_end(&before, life), life.until) {
                    shown.borrow_mut().push(Shown {
                        session_id: life.session_id,
                        until,
                        pick: cx.pick.clone(),
                        now: since_start(cx.now),
                        reactor_event: cx
                            .observations()
                            .any(|observation| matches!(observation, Observation::Reactor { .. })),
                        sessions: view.sessions.len(),
                    });
                }
            }
            None
        });
        let report = run(
            Storage::config(&scenario),
            input,
            |sim| {
                let ports = Storage::prepare(&scenario);
                *storage.borrow_mut() = Some(Rc::new(Storage::build(sim, &scenario, ports)));
            },
            &mut checker,
        );
        drop(checker);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let shown = shown.into_inner();
        // Each mirror's first link: `mirror-0`'s is connection 1 and
        // `mirror-1`'s connection 2, the session IDs the same.
        for session_id in [1, 2] {
            let first = first_shown(&shown, &SessionIdRef { session_id })
                .unwrap_or_else(|| panic!("session {session_id} ended: {shown:#?}"));
            assert_eq!(first.pick.category, Category::Task, "{first:?}");
            // The control task, numbered by the scheduler.
            assert_eq!(
                first.pick.key.split('.').next(),
                Some("control"),
                "{first:?}"
            );
            assert!(!first.reactor_event, "{first:?}");
            assert_eq!(first.until, first.now, "{first:?}");
            // The end is the close's, not the relink's replacement: the
            // view doesn't hold the mirror's next session yet.
            assert_eq!(
                first.sessions,
                usize::try_from(session_id + 1).expect("two sessions"),
                "{first:?}"
            );
        }
    }
}

/// The end the earlier view `before` showed for `life`'s session, joined
/// by its [`SessionIdRef`], or `None` if it showed none.
fn earlier_end(before: &[SessionLife], life: &SessionLife) -> Option<Duration> {
    let session = SessionIdRef {
        session_id: life.session_id,
    };
    SessionIdRef::joined(&session, before, |earlier| {
        Some(SessionIdRef {
            session_id: earlier.session_id,
        })
    })
    .first()
    .and_then(|earlier| earlier.until)
}

/// The first view that showed `session`'s end, joined by its
/// [`SessionIdRef`].
fn first_shown<'s>(shown: &'s [Shown], session: &SessionIdRef) -> Option<&'s Shown> {
    SessionIdRef::joined(session, shown, |shown| {
        Some(SessionIdRef {
            session_id: shown.session_id,
        })
    })
    .first()
    .copied()
}

/// The view joins by its session: session 1's end shows only beside its
/// own earlier life and first view, and a life or view of a session one
/// key off is another's.
#[test]
fn a_view_joins_its_earlier_life_by_its_session() {
    let session = SessionIdRef { session_id: 1 };
    let ended = SessionLife {
        session_id: 1,
        from: Duration::ZERO,
        until: Some(Duration::from_secs(1)),
    };
    assert_one_key_off::<SessionIdRef>(&session, |earlier| {
        let before = [SessionLife {
            session_id: earlier.session_id,
            ..ended
        }];
        earlier_end(&before, &ended).is_some()
    });
    assert_one_key_off::<SessionIdRef>(&session, |shown| {
        let views = [Shown {
            session_id: shown.session_id,
            until: Duration::from_secs(1),
            pick: Identity::advance_time(SimInstant::ZERO),
            now: Duration::from_secs(1),
            reactor_event: false,
            sessions: 2,
        }];
        first_shown(&views, &session).is_some()
    });
}

/// Ends by connection ID, as virtual time since the run's start.
type Ends = BTreeMap<u64, Duration>;

/// Whether `ended` holds exactly `cancelled`'s ends: each joined by its
/// link's [`ConnectionRef`], at the same time, and no other.
fn same_ends(cancelled: &Ends, ended: &Ends) -> bool {
    let recorded: Vec<(u64, Duration)> = ended.iter().map(|(link, at)| (*link, *at)).collect();
    ended.len() == cancelled.len()
        && cancelled.iter().all(|(connection, at)| {
            let link = ConnectionRef {
                connection: *connection,
            };
            let joined = ConnectionRef::joined(&link, &recorded, |(connection, _)| {
                Some(ConnectionRef {
                    connection: *connection,
                })
            });
            matches!(joined[..], [(_, end)] if end == at)
        })
}

/// The recorded and cancelled ends join by their link: connection 1's end
/// at 1s matches only an end recorded for connection 1, and one for a
/// connection one key off is another link's.
#[test]
fn an_end_joins_its_cancellation_by_its_connection() {
    let link = ConnectionRef { connection: 1 };
    let cancelled = Ends::from([(1, Duration::from_secs(1))]);
    assert_one_key_off::<ConnectionRef>(&link, |recorded| {
        same_ends(
            &cancelled,
            &Ends::from([(recorded.connection, Duration::from_secs(1))]),
        )
    });
    assert!(!same_ends(
        &cancelled,
        &Ends::from([(1, Duration::from_secs(2))])
    ));
}

/// The node at genesis with `mirror-0` at A5, dialing it, and `mirror-1`
/// at A4, dialed by it, under the converging policy, run by `input`, with
/// close-and-relink's control script, its closes held to 1s and 2s so each
/// end has its own time. Returns, after each step, the step, each link's
/// connection with when its tokens were first seen cancelled, and the
/// topology's recorded ends.
fn ends_run(input: zakura_sim::PlanInput) -> Vec<(u64, Ends, Ends)> {
    let fixture = HeaderFixture::regtest();
    let node = Arc::new(ControlledPort::new(&fixture));
    let mirrors = [Chain::main(5), Chain::main(4)].map(|chain| {
        let port = Arc::new(EnginePort::new(&fixture));
        preload(&port, &fixture, &chain);
        (port, chain)
    });
    let effects = Effects::new(&EvidenceStores::default());
    let cancelled: RefCell<Ends> = RefCell::default();
    let seen = RefCell::new(Vec::new());
    let mut checker = after_step(|cx| {
        let mut cancelled = cancelled.borrow_mut();
        for link in effects.links.borrow().iter() {
            if link.is_closed() {
                cancelled.entry(link.conn_id).or_insert(since_start(cx.now));
            }
        }
        seen.borrow_mut()
            .push((cx.step, cancelled.clone(), effects.session_ends()));
        None
    });
    let mut topology = None;
    let report = run(
        storage_config(),
        input,
        |sim| {
            let dispatch = DispatchFlag::default();
            let storage =
                PolicySource::new(sim, node.clone(), Policy::converging(), effects.clone());
            let (observer, tap) = ReactorTap::new(sim, &node, "node");
            let storage = storage.with_tap(tap);
            let predicates = storage.predicates();
            sim.add_source(Box::new(storage));
            let mut nodes = vec![HeaderNode::spawn(
                sim,
                "node",
                1,
                node.clone(),
                &fixture,
                Chain::default().tip(&fixture),
                dispatch.clone(),
                Some(observer),
            )];
            for (index, (port, chain)) in mirrors.iter().enumerate() {
                nodes.push(HeaderNode::spawn(
                    sim,
                    &format!("mirror-{index}"),
                    u8::try_from(index + 2).expect("two mirrors"),
                    port.clone(),
                    &fixture,
                    chain.tip(&fixture),
                    dispatch.clone(),
                    None,
                ));
            }
            let linked = Rc::new(Topology::new(nodes, effects.clone()));
            linked.link(1, 0);
            linked.link(0, 2);
            let controls = vec![
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::VirtualTime(1_000)),
                ),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::Closed("mirror-0".to_owned())),
                ),
                step(
                    "mirror-1",
                    ControlAction::Close,
                    Some(Trigger::All(vec![
                        Trigger::Count(2, Box::new(Trigger::Linked("mirror-0".to_owned()))),
                        Trigger::VirtualTime(2_000),
                    ])),
                ),
                step(
                    "mirror-1",
                    ControlAction::Relink,
                    Some(Trigger::Closed("mirror-1".to_owned())),
                ),
            ];
            let control = Control::new(sim, linked.clone(), fixture.clone(), controls, predicates);
            sim.add_source(Box::new(control));
            topology = Some(linked);
        },
        &mut checker,
    );
    drop(checker);
    drop(topology);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    seen.into_inner()
}

/// [`Effects::ended`]'s producer test, which also covers the fork-choice
/// check's `LinkEnds::ended`, a second handle on the same store. After
/// every step the recorded ends are exactly the links whose tokens are
/// cancelled, each at the time of the step that cancelled them. By the
/// run's last step that's each mirror's first link, which its `Close`
/// step ended, and neither relink.
#[test]
fn each_recorded_end_is_its_links_cancellation() {
    for input in inputs(8, 256) {
        let seen = ends_run(input);
        for (step, cancelled, ended) in &seen {
            assert!(
                same_ends(cancelled, ended),
                "after step {step}: {ended:?} recorded, {cancelled:?} cancelled"
            );
        }
        let (_, last, _) = seen.last().expect("the run took steps");
        assert_eq!(last.keys().copied().collect::<Vec<_>>(), [1, 2], "{last:?}");
        assert!(last[&1] >= Duration::from_secs(1), "{last:?}");
        assert!(last[&2] >= Duration::from_secs(2), "{last:?}");
    }
}
