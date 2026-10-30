//! The header sessions on `header-sync/wire` (M6a-7b): the session table of
//! the header-sync service the node's registry holds, read at every step
//! boundary with no scheduling point, and the install, refusal and removal
//! records each registry fanout and table change makes. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_network::zakura::{header_stream_bypass, ZAKURA_CAP_HEADER_SYNC};
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            reads::without_reads,
            Chain, Policy,
        },
        header_sync::{HeaderWire, WireScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{ComponentReading, ComponentValue, ReactorFact, SessionRow, TransportFact},
    peer::{honest::HonestSpec, PeerAction, PeerSpec},
    MemorySink, Observation, Plan, PlanInput, RunReport,
};

/// `peer-0`, an honest peer with 120 headers, dials the node at 50.
fn honest() -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: None,
            }),
            ..PeerSpec::dialing()
        }],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// The node at 50 dials `peer-0`, which holds the 1,200-header tree and
/// finishes its side of the header stream once every subscription has
/// ended, so the stream's session tears down and a new one reopens on the
/// same connection (`header_wire_view.rs`'s reopen).
fn reopen() -> WireScenario {
    let mut peer = PeerSpec {
        honest: Some(HonestSpec {
            chain: 1_200,
            subscribe: None,
        }),
        ..PeerSpec::listening()
    };
    peer.script = vec![PeerAction::FinishHeader];
    WireScenario {
        node: Chain::main(50),
        peers: vec![peer],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 2),
                until: Trigger::VirtualTime(5_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// The plans these tests run: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// Runs `scenario` under `plan`, and returns the report, even of a run that
/// ended in a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    let ports = HeaderWire::prepare(scenario);
    let mut sink = MemorySink::default();
    match run_prepared::<HeaderWire>(scenario, ports, plan, &mut sink, None) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// One header-sessions read the hub logged: its place in the log, its
/// step, and what it read.
struct SessionsRead<'r> {
    at: usize,
    step: u64,
    read: &'r ComponentReading,
}

impl SessionsRead<'_> {
    /// The service it names and its sessions, if it's current.
    fn current(&self) -> Option<(u64, &[SessionRow])> {
        match self.read {
            ComponentReading::Current(ComponentValue::HeaderSessions { service, sessions }) => {
                Some((*service, sessions))
            }
            _ => None,
        }
    }

    /// Whether a task held the table.
    fn held(&self) -> bool {
        matches!(self.read, ComponentReading::Held { .. })
    }
}

/// The header sessions' logged reads.
fn session_reads(report: &RunReport) -> Vec<SessionsRead<'_>> {
    report
        .log
        .entries()
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::ComponentRead {
                component, read, ..
            } if component == "HeaderSessions" => Some(SessionsRead {
                at,
                step: entry.step,
                read,
            }),
            _ => None,
        })
        .collect()
}

/// Each transport row of `report` with its place, step and connection.
fn transport(report: &RunReport) -> Vec<(usize, u64, Option<u64>, &TransportFact)> {
    report
        .log
        .entries()
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Transport {
                connection, event, ..
            } => Some((at, entry.step, *connection, event)),
            _ => None,
        })
        .collect()
}

/// The place of each fanout of `connection` that reached header sync.
fn header_fanouts(report: &RunReport, connection: u64) -> Vec<usize> {
    transport(report)
        .into_iter()
        .filter_map(|(at, _, c, event)| match event {
            TransportFact::RegistryFanout { admitted_mask, .. }
                if c == Some(connection) && admitted_mask & ZAKURA_CAP_HEADER_SYNC != 0 =>
            {
                Some(at)
            }
            _ => None,
        })
        .collect()
}

/// The task stop logged at `step`'s boundary, as its site, if a task ran
/// to a point.
fn stop_site(report: &RunReport, step: u64) -> Option<&str> {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::TaskReached { site, .. } if entry.step == step => site.as_deref(),
            _ => None,
        })
}

/// Every task stop the run logged, with its step, op, object and site.
fn stops(report: &RunReport) -> Vec<(u64, &Observation)> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.obs.ran_task().is_some())
        .map(|entry| (entry.step, &entry.obs))
        .collect()
}

/// A bypass reads the honest peer's header stream as version 8 after the
/// registry selected header sync, so `add_peer` refuses it at its stream
/// check. The run first shows that refusal, `regulation_contract`, and the
/// fanout after it, whose mask has header sync's bit. No session is ever
/// installed, and every table read is current and empty.
///
/// This fails if the installed state comes from the fanout's mask.
#[test]
fn a_refused_fanout_installs_nothing() {
    let report =
        header_stream_bypass::with_stream_version(8, || run(&honest(), Plan::from_fuzz(&[])));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let rows = transport(&report);
    let refused = rows
        .iter()
        .find_map(|(at, _, connection, event)| match event {
            TransportFact::InstallRefused { service: 2, reason }
                if reason == "regulation_contract" =>
            {
                Some((*at, connection.expect("a refusal names its connection")))
            }
            _ => None,
        })
        .expect("add_peer refuses the stream");
    let (at, connection) = refused;
    assert!(
        header_fanouts(&report, connection)
            .iter()
            .any(|fanout| *fanout > at),
        "the refusing fanout reached header sync"
    );
    assert!(!rows
        .iter()
        .any(|(_, _, _, event)| matches!(event, TransportFact::Installed { .. })));
    let reads = session_reads(&report);
    assert!(!reads.is_empty());
    for read in reads {
        let (service, sessions) = read.current().expect("nothing holds the table");
        assert_eq!(service, 2);
        assert!(
            sessions.is_empty(),
            "step {}: the table installs nothing: {sessions:?}",
            read.step
        );
    }
}

/// An honest connection's `installed` comes inside its fanout: after the
/// connection's earlier fanout, which opened nothing, and before the one
/// that reached header sync. The run first shows both fanouts and the
/// install, and the hub's fanout rule accepts it.
///
/// This fails if the rule wants the install after its fanout.
#[test]
fn an_install_precedes_its_fanout_record() {
    for plan in plans() {
        let report = run(&honest(), plan);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let rows = transport(&report);
        let (installed, connection) = rows
            .iter()
            .find_map(|(at, _, connection, event)| match event {
                TransportFact::Installed { .. } => Some((*at, connection.expect("named"))),
                _ => None,
            })
            .expect("the node installs the honest session");
        let fanouts: Vec<(usize, u64)> = rows
            .iter()
            .filter_map(|(at, _, c, event)| match event {
                TransportFact::RegistryFanout { admitted_mask, .. } if *c == Some(connection) => {
                    Some((*at, *admitted_mask))
                }
                _ => None,
            })
            .collect();
        let next = fanouts
            .iter()
            .position(|(at, _)| *at > installed)
            .expect("a fanout follows the install");
        assert!(next > 0, "an earlier fanout opened nothing: {fanouts:?}");
        assert_eq!(fanouts[next - 1].1 & ZAKURA_CAP_HEADER_SYNC, 0);
        assert_ne!(fanouts[next].1 & ZAKURA_CAP_HEADER_SYNC, 0);
    }
}

/// The wire harness builds two header-sync services on the node's reactor
/// handle, the `HeaderNode`'s and the registry's, and each registers its
/// table's read. The run first shows both registrations. The hub reads the
/// registry's, service 2, which installs the honest session: every table
/// read names service 2, and one shows the session.
///
/// This fails if the hub reads the first service built.
#[test]
fn the_wire_view_reads_the_registered_service() {
    let report = run(&honest(), Plan::from_fuzz(&[]));
    let registered: Vec<&str> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                event: ReactorFact::ReadRegistered { key, .. },
                ..
            } if key.starts_with("header-sessions/") => Some(key.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(registered, ["header-sessions/1", "header-sessions/2"]);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let reads = session_reads(&report);
    assert!(reads
        .iter()
        .filter_map(SessionsRead::current)
        .all(|(service, _)| service == 2));
    assert!(
        reads
            .iter()
            .filter_map(SessionsRead::current)
            .any(|(_, sessions)| sessions.len() == 1),
        "the registry's service installs the session"
    );
}

/// Each `installed` is logged at the insert, under the table's lock: at
/// its step's boundary the installing task is stopped at `replace`'s watch
/// send, after the insert and before the unlock, and the table reads held.
/// No read shows the session before its `installed`. The runs first show
/// an install on a fresh connection, and on a header stream reopened on a
/// live one.
///
/// This fails if `installed` is reported once `replace` releases the lock.
#[test]
fn the_install_is_recorded_at_the_insert() {
    for scenario in [honest(), reopen()] {
        let report = run(&scenario, Plan::from_fuzz(&[]));
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let reads = session_reads(&report);
        let installs: Vec<(usize, u64, u64)> = transport(&report)
            .into_iter()
            .filter_map(|(at, step, _, event)| match event {
                TransportFact::Installed { session_id, .. } => Some((at, step, *session_id)),
                _ => None,
            })
            .collect();
        assert!(!installs.is_empty(), "the node installs sessions");
        for (at, step, session) in installs {
            assert!(
                stop_site(&report, step)
                    .is_some_and(|site| site.contains("regulation/session_table.rs")),
                "step {step}: the install stops inside the table: {:?}",
                stop_site(&report, step)
            );
            assert!(
                reads.iter().any(|read| read.step == step && read.held()),
                "step {step}: the table reads held at the install"
            );
            let first_shown = reads.iter().find(|read| {
                read.current().is_some_and(|(_, sessions)| {
                    sessions.iter().any(|row| row.session_id == session)
                })
            });
            assert!(
                first_shown.is_some_and(|read| read.at > at),
                "session {session}: no read shows it before its install"
            );
        }
    }
}

/// On the reopen history, the first stream's teardown removes its session
/// with `remove`, which stops at the session's fence lock while it holds
/// the table's. At that boundary the table reads held, so the component is
/// in transition, never current with either the old or the emptied table.
/// The read before shows the session, and the next current one doesn't.
/// The run first shows the teardown's `removed` and that stop.
///
/// This fails if a held table is reported as current.
#[test]
fn a_pause_inside_remove_is_in_transition() {
    let report = run(&reopen(), Plan::from_fuzz(&[]));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let (step, session) = transport(&report)
        .into_iter()
        .find_map(|(_, step, _, event)| match event {
            TransportFact::Removed {
                session_id, cause, ..
            } if cause == "teardown" => Some((step, *session_id)),
            _ => None,
        })
        .expect("the first stream's teardown removes its session");
    assert!(
        stop_site(&report, step).is_some_and(|site| site.contains("regulation/writer_fence.rs")),
        "step {step}: the removal stops at the fence's lock: {:?}",
        stop_site(&report, step)
    );
    let reads = session_reads(&report);
    let at = reads
        .iter()
        .position(|read| read.step == step)
        .expect("the table's read changed at the removal");
    assert!(reads[at].held(), "step {step}: the table reads held");
    let shows = |read: &SessionsRead<'_>| {
        read.current()
            .map(|(_, sessions)| sessions.iter().any(|row| row.session_id == session))
    };
    assert_eq!(
        shows(&reads[at - 1]),
        Some(true),
        "the read before shows it"
    );
    let next = reads[at + 1..]
        .iter()
        .find(|read| read.current().is_some())
        .expect("a later read is current");
    assert_eq!(shows(next), Some(false), "the next current read doesn't");
}

/// The table's reads add no scheduling point: under each plan, the honest
/// and reopen histories take the same picks and task stops, with each
/// stop's op, object and site, and end the same, with the boundary reads
/// as without them. The runs with reads first show the table read current
/// with a session in it, and held while a task holds its lock.
///
/// This fails if the read takes the table's lock, which is a scheduling
/// point, or keeps it past the read.
#[test]
fn table_reads_add_no_scheduling_point() {
    for scenario in [honest(), reopen()] {
        for plan in plans() {
            let read = run(&scenario, plan.clone());
            let unread = without_reads(|| run(&scenario, plan));
            let reads = session_reads(&read);
            assert!(
                reads.iter().any(|read| read
                    .current()
                    .is_some_and(|(_, sessions)| !sessions.is_empty())),
                "the table reads a session"
            );
            assert!(reads.iter().any(SessionsRead::held), "the table reads held");
            assert!(session_reads(&unread).is_empty());
            assert_eq!(read.end, unread.end);
            assert_eq!(support::picks(&read), support::picks(&unread));
            assert_eq!(stops(&read), stops(&unread));
        }
    }
}
