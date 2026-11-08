//! hdr-9a: the node's end facts on `header-sync/storage`. The harness
//! records each environment action where it acts, a link the node's own
//! service cancels records nothing, and every view-side fact takes the
//! node's next stamp. A reactor that returns by itself is recorded, not
//! failed (`tests/header_pairing.rs`).
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, io::Write, path::Path, process::Command};

use serde_json::Value;
use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        self,
        header_node::{
            ends::{EndEvidence, EndFact, EnvironmentAction},
            policy::{Allow, Hold, OpKind, Select, Trigger},
            session::SessionFact,
            ControlAction, ControlStep, Policy,
        },
        header_sync::{Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    MemorySink, PlanInput, RunReport,
};

/// What [`run`] returns: the report, the node's end facts with their
/// stamps, its session facts, and every stamp the node handed out.
struct Ran {
    report: RunReport,
    ends: Vec<(u64, EndFact)>,
    sessions: Vec<SessionFact>,
    stamped: Vec<u64>,
}

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Ran {
    let ports = Storage::prepare(scenario);
    let ends = ports.ends.clone();
    let sessions = ports.sessions.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    Ran {
        report,
        ends: ends.facts(),
        sessions: sessions.facts(),
        stamped: ends.stamps().after(None),
    }
}

/// The connections of `peer`'s links, in the order they opened.
fn links(sessions: &[SessionFact], peer: &str) -> Vec<u64> {
    sessions
        .iter()
        .filter_map(|fact| match fact {
            SessionFact::LinkOpened {
                peer: linked,
                connection,
                ..
            } if linked == peer => Some(*connection),
            _ => None,
        })
        .collect()
}

/// The connection of `peer`'s first link.
fn first_link(sessions: &[SessionFact], peer: &str) -> u64 {
    links(sessions, peer)
        .first()
        .copied()
        .unwrap_or_else(|| panic!("{peer} is linked: {sessions:#?}"))
}

/// The connection of `peer`'s second link, its relink.
fn second_link(sessions: &[SessionFact], peer: &str) -> u64 {
    links(sessions, peer)
        .get(1)
        .copied()
        .unwrap_or_else(|| panic!("{peer} is relinked: {sessions:#?}"))
}

/// The environment fact for `action` on `peer`'s `connection`.
fn environment(peer: &str, connection: Option<u64>, action: EnvironmentAction) -> EndFact {
    EndFact::Environment {
        peer: peer.to_owned(),
        connection,
        action,
    }
}

/// The facts of `ends`, without their stamps.
fn facts(ends: &[(u64, EndFact)]) -> Vec<EndFact> {
    ends.iter().map(|(_, fact)| fact.clone()).collect()
}

/// In the close-and-relink scenario, each mirror's `Close` and `Relink` step
/// records its end once, in script order: the close names the mirror's
/// first link, and the relink names the new link it opens. The reactor
/// never exits by itself.
#[test]
fn close_and_relink_steps_record_the_connections_they_end() {
    let scenario = StorageScenario::close_relink();
    for input in inputs(8, 256) {
        let ran = run(&scenario, input);
        let zero = first_link(&ran.sessions, "mirror-0");
        let one = first_link(&ran.sessions, "mirror-1");
        assert_eq!(
            facts(&ran.ends),
            [
                environment("mirror-0", Some(zero), EnvironmentAction::Close),
                environment(
                    "mirror-0",
                    Some(second_link(&ran.sessions, "mirror-0")),
                    EnvironmentAction::Relink
                ),
                environment("mirror-1", Some(one), EnvironmentAction::Close),
                environment(
                    "mirror-1",
                    Some(second_link(&ran.sessions, "mirror-1")),
                    EnvironmentAction::Relink
                ),
            ],
            "{:#?}",
            ran.sessions
        );
    }
}

/// A relink that replaces `mirror-0`'s session while its first prepare is
/// held, without a close step. Under some plans the node's own service
/// then closes the first link, which the session view sees as
/// `LinkClosed`. The harness records only its relink: a connection the
/// node's service cancels records no end.
#[test]
fn a_service_side_close_records_no_end() {
    let scenario = StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted("mirror-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Relink,
            when: Some(Trigger::Submitted(Select::nth(OpKind::Prepare, 1))),
        }],
        ..StorageScenario::default()
    };
    let mut service_closed = 0;
    for input in inputs(16, 256) {
        let ran = run(&scenario, input);
        let first = first_link(&ran.sessions, "mirror-0");
        assert_eq!(
            facts(&ran.ends),
            [environment(
                "mirror-0",
                Some(second_link(&ran.sessions, "mirror-0")),
                EnvironmentAction::Relink
            )],
        );
        if ran.sessions.contains(&SessionFact::LinkClosed {
            peer: "mirror-0".to_owned(),
            connection: first,
        }) {
            service_closed += 1;
        }
    }
    assert!(
        service_closed > 0,
        "no plan's service closed the first link"
    );
}

/// A churn and a `Relink` of the same mirror overlap, so two relinks can be
/// open at once. Each relink names its own new connection, never another
/// relink's: every link after the first has exactly one relink fact, and
/// every close fact names a link that opened. A shutdown stops the churn.
#[test]
fn overlapping_relinks_each_name_their_own_connection() {
    let admitted = |count| {
        Some(Trigger::Count(
            count,
            Box::new(Trigger::SessionAdmitted("mirror-0".to_owned())),
        ))
    };
    let scenario = StorageScenario {
        node: zakura_sim::harness::header_node::Chain::main(4),
        mirrors: vec![zakura_sim::harness::header_sync::MirrorSpec {
            chain: zakura_sim::harness::header_node::Chain::main(4),
            dialer: zakura_sim::harness::header_sync::Dialer::Mirror,
        }],
        controls: vec![
            ControlStep {
                peer: "mirror-0".to_owned(),
                action: ControlAction::Churn,
                when: None,
            },
            ControlStep {
                peer: "mirror-0".to_owned(),
                action: ControlAction::Relink,
                when: admitted(3),
            },
            ControlStep {
                peer: "node".to_owned(),
                action: ControlAction::Shutdown,
                when: admitted(6),
            },
        ],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let ran = run(&scenario, input);
        let opened = links(&ran.sessions, "mirror-0");
        let relinked: Vec<u64> = ran
            .ends
            .iter()
            .filter_map(|(_, fact)| match fact {
                EndFact::Environment {
                    action: EnvironmentAction::Relink,
                    connection,
                    ..
                } => *connection,
                _ => None,
            })
            .collect();
        assert_eq!(relinked, opened[1..], "{:#?}", ran.ends);
        for (_, fact) in &ran.ends {
            if let EndFact::Environment {
                action: EnvironmentAction::Close,
                connection: Some(connection),
                ..
            } = fact
            {
                assert!(opened.contains(connection), "{connection}: {opened:?}");
            }
        }
    }
}

/// A `Shutdown` step records one end for the node under test, with no
/// connection, at its pick. The links its stop closes record nothing more,
/// and a reactor the harness stopped isn't recorded as exited.
#[test]
fn a_shutdown_records_one_end_at_its_pick() {
    let scenario = StorageScenario {
        controls: vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Shutdown,
            when: Some(Trigger::Submitted(Select::nth(OpKind::Prepare, 1))),
        }],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let ran = run(&scenario, input);
        assert_eq!(
            facts(&ran.ends),
            [environment("node", None, EnvironmentAction::Shutdown)]
        );
        assert!(ran
            .report
            .picks
            .iter()
            .any(|pick| pick.identity == "peer:node:shutdown"));
    }
}

/// The end facts take the node's stamps, which other stores take too: the
/// adoption store stamps the reactor's startup before any end, so the
/// first end's stamp isn't 1, the ends' stamps increase, and none is past
/// the last one handed out. The stamps depend only on the scenario and the
/// plan, so the same plan stamps the same facts again.
#[test]
fn end_facts_take_the_node_stamps() {
    let scenario = StorageScenario::close_relink();
    for input in inputs(4, 256) {
        let ran = run(&scenario, input.clone());
        let ends: Vec<u64> = ran.ends.iter().map(|(stamp, _)| *stamp).collect();
        assert!(ends.windows(2).all(|pair| pair[0] < pair[1]), "{ends:?}");
        assert!(ends.first().is_some_and(|first| *first > 1), "{ends:?}");
        let last = ran.stamped.last().copied().unwrap_or_default();
        assert!(ends.iter().all(|stamp| *stamp <= last), "{ends:?} {last}");
        let again = run(&scenario, input);
        assert_eq!(
            (ran.ends, ran.stamped),
            (again.ends, again.stamped),
            "the same plan stamps the same facts"
        );
    }
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_stamps_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_ENDS_TRACE";

thread_local! {
    /// The end facts of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<EndEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its end facts in [`CAPTURED`].
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.ends.clone()));
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// The end facts [`capture_run`] last kept, with their stamps and the last
/// stamp handed out, as text.
fn captured() -> String {
    let ends = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(!ends.facts().is_empty());
    format!(
        "{:?} {:?}\n",
        ends.facts(),
        ends.stamps().after(None).last()
    )
}

/// The close-and-relink scenario, recorded in this process and replayed
/// exactly in a fresh process of the same test binary, records the same
/// end facts with the same stamps, and hands out the same last stamp.
#[test]
fn the_stamps_replay_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(Storage::NAME).expect("the storage harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        writeln!(std::io::stdout(), "ENDS-BEGIN\n{}ENDS-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    let scenario =
        serde_json::to_value(StorageScenario::close_relink()).expect("a scenario serializes");
    record(
        &entry,
        RecordInput::Plan {
            scenario: &scenario,
            plan: &[3; 256],
        },
        dir.path(),
    )
    .expect("the scenario records");
    let recorded = captured();
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_stamps_replay_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("ENDS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("ENDS-END"))
        .map(|(ends, _)| ends)
        .unwrap_or_else(|| panic!("the child prints its end facts: {stdout}"));
    assert_eq!(replayed, recorded);
}
