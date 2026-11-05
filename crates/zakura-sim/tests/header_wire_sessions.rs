//! hdr-5's join of the reactor's sessions to own-9's facts on
//! `header-sync/wire`. Each run checks that every session the node's reactor
//! admits matches exactly one own-9 `Session` row and one installed header
//! generation, by the origin its handler set. These witnesses show the join
//! across a close and redial, and across two generations on one connection,
//! and that own-9 retires each session the reactor lets go.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, io::Write, path::Path, process::Command};

use serde_json::Value;
use support::inputs;
use tempfile::TempDir;
use zakura_network::zakura::SessionOrigin;
use zakura_sim::{
    harness::{
        self,
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            session::{EndCheck, Retirement, SessionEvidence, SessionFact},
            Chain, Policy,
        },
        header_sync::{HeaderWire, WireScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    peer::{
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        PeerAction, PeerSpec,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    MemorySink, PlanInput, RunReport,
};

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure. Returns its report, the node's
/// session facts and the view's end check.
fn run(scenario: &WireScenario, input: PlanInput) -> (RunReport, Vec<SessionFact>, EndCheck) {
    let ports = HeaderWire::prepare(scenario);
    let sessions = ports.sessions.clone();
    let report =
        run_prepared::<HeaderWire>(scenario, ports, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, sessions.facts(), sessions.end_check())
}

/// An honest peer holding `chain` headers, as `spec`, subscribing as
/// `subscribe`, with `script`.
fn honest(
    spec: PeerSpec,
    chain: u32,
    subscribe: Option<SubscribeSpec>,
    script: Vec<PeerAction>,
) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        script,
        ..spec
    }
}

/// The close-and-redial witness: `peer-0` dials the node, closes its
/// connection, and dials again from a fresh address.
fn redial_witness() -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![honest(
            PeerSpec::dialing(),
            120,
            None,
            vec![PeerAction::Close, PeerAction::Redial],
        )],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// The two-generation witness: the node dials `peer-0`, both subscribe, and
/// the peer finishes its header stream once both subscriptions end, so the
/// node reopens it on the same connection. The node's next locator is held,
/// so its second subscription doesn't race the finish.
fn reopen_witness() -> WireScenario {
    let subscribe = SubscribeSpec {
        start: 0,
        open: CreditSpec {
            objects: 250,
            bytes: 65_750,
        },
        grants: GrantPolicy::Immediate,
    };
    WireScenario {
        node: Chain::main(50),
        peers: vec![honest(
            PeerSpec::listening(),
            120,
            Some(subscribe),
            vec![PeerAction::FinishHeader],
        )],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 2),
                until: Trigger::VirtualTime(3_600_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// Each admission in `facts`: its session ID, the session it replaced and
/// the origin its handler set.
fn admissions(facts: &[SessionFact]) -> Vec<(u64, Option<u64>, SessionOrigin)> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            SessionFact::SessionAdmitted {
                peer,
                session_id,
                replaced,
                origin,
                ..
            } => {
                assert_eq!(peer, "peer-0", "{facts:#?}");
                Some((
                    *session_id,
                    *replaced,
                    origin.expect("the handler sets every origin"),
                ))
            }
            _ => None,
        })
        .collect()
}

/// How own-9 retired session `session_id` in `facts`, if it did.
fn retired(facts: &[SessionFact], session_id: u64) -> Option<Retirement> {
    facts.iter().find_map(|fact| match fact {
        SessionFact::SessionRetired {
            session_id: id, by, ..
        } if *id == session_id => Some(*by),
        _ => None,
    })
}

/// A close then a redial: each connection's session is joined to its own
/// own-9 row and generation, and the second replaces nothing, because the
/// node refuses a newcomer for a live identity and so tears the first
/// connection down before the second registers. That teardown retires the
/// first session.
#[test]
fn a_close_and_redial_joins_each_connection_once() {
    for input in inputs(3, 16_384) {
        let (_, facts, end) = run(&redial_witness(), input);
        let [first, second] = admissions(&facts)[..] else {
            panic!("two admissions: {facts:#?}");
        };
        assert_eq!((first.1, second.1), (None, None), "{facts:#?}");
        assert_ne!(first.2.connection, second.2.connection, "{facts:#?}");
        assert_ne!(first.0, second.0, "{facts:#?}");
        for (session_id, _, origin) in [first, second] {
            assert_eq!(origin.generation, session_id, "{facts:#?}");
        }
        assert_eq!(
            retired(&facts, first.0),
            Some(Retirement::TornDown),
            "{facts:#?}"
        );
        assert_eq!(end, EndCheck::Complete, "{facts:#?}");
    }
}

/// Two generations on one connection: the reopened session is joined to its
/// own QUIC stream and generation on the first's connection, and own-9
/// retires the first by its generation's exit, since the connection stays
/// up.
#[test]
fn a_reopen_joins_a_second_generation_on_one_connection() {
    for input in inputs(3, 16_384) {
        let (_, facts, end) = run(&reopen_witness(), input);
        let [first, second] = admissions(&facts)[..] else {
            panic!("two admissions: {facts:#?}");
        };
        assert_eq!(first.2.connection, second.2.connection, "{facts:#?}");
        assert_ne!(first.2.quic_stream, second.2.quic_stream, "{facts:#?}");
        assert_ne!(first.2.generation, second.2.generation, "{facts:#?}");
        assert_eq!(
            retired(&facts, first.0),
            Some(Retirement::GenerationExited),
            "{facts:#?}"
        );
        assert_eq!(end, EndCheck::Complete, "{facts:#?}");
    }
}

/// Both witnesses' session facts and full logs depend only on scenario and
/// plan: plan A twice in a row, then B, then A again, gives A's every time,
/// and B's log differs.
#[test]
fn the_wire_session_witnesses_repeat_a_a_and_a_b_a() {
    let plans = inputs(3, 16_384);
    for scenario in [redial_witness(), reopen_witness()] {
        let runs: Vec<(String, Vec<SessionFact>)> = [&plans[1], &plans[1], &plans[2], &plans[1]]
            .into_iter()
            .map(|input| {
                let (report, facts, _) = run(&scenario, input.clone());
                (format!("{:?}", report.log.entries()), facts)
            })
            .collect();
        assert_eq!(runs[0], runs[1], "plan A, then A");
        assert_eq!(runs[0], runs[3], "plan A, then B, then A");
        assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
    }
}

/// Set, to a directory of recorded traces, in the child process that
/// [`the_wire_session_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_WIRE_SESSION_TRACES";

thread_local! {
    /// The session view of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<SessionEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/wire`'s own run, keeping its session view in [`CAPTURED`]
/// for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: WireScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = HeaderWire::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.sessions.clone()));
    run_prepared::<HeaderWire>(&scenario, ports, input, sink, None)
}

/// `header-sync/wire`'s registry entry, with its run replaced by
/// [`capture_run`].
fn capturing() -> HarnessEntry {
    HarnessEntry {
        run: capture_run,
        ..*harness::find(HeaderWire::NAME).expect("the wire harness is registered")
    }
}

/// The session view [`capture_run`] last kept, with its end check, as
/// text.
fn captured() -> String {
    let view = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    format!("{:?}\n{:?}\n", view.facts(), view.end_check())
}

/// The witnesses, by the name their traces are recorded under.
fn witnesses() -> [(&'static str, WireScenario); 2] {
    [("redial", redial_witness()), ("reopen", reopen_witness())]
}

/// Each witness recorded under one plan in this process, then replayed
/// exactly in a fresh process of the same test binary, gives the session
/// facts and end check its recorded run gave.
#[test]
fn the_wire_session_witnesses_replay_in_a_fresh_process() {
    let entry = capturing();
    if let Some(dir) = std::env::var_os(CHILD) {
        let facts: String = witnesses()
            .into_iter()
            .map(|(name, _)| {
                let trace =
                    TraceReader::open(&Path::new(&dir).join(name)).expect("the trace was recorded");
                // A replay that leaves the trace's picks, draws or
                // observations, or fails where the recording didn't, errs.
                replay_with(&trace, &entry, ReplayOptions::default())
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
                captured()
            })
            .collect();
        // The parent reads the facts from the child's stdout.
        writeln!(std::io::stdout(), "FACTS-BEGIN\n{facts}FACTS-END")
            .expect("the test harness captures stdout");
        return;
    }
    let PlanInput::Bytes(plan) = inputs(1, 16_384).remove(0) else {
        unreachable!("fuzz inputs are bytes");
    };
    let dir = TempDir::new().expect("a temporary directory");
    let mut recorded = String::new();
    for (name, scenario) in witnesses() {
        let scenario = serde_json::to_value(&scenario).expect("a scenario serializes");
        let what = RecordInput::Plan {
            scenario: &scenario,
            plan: &plan,
        };
        record(&entry, what, &dir.path().join(name)).expect("the witness records");
        recorded.push_str(&captured());
    }
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_wire_session_witnesses_replay_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("FACTS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("FACTS-END"))
        .map(|(facts, _)| facts)
        .unwrap_or_else(|| panic!("the child prints its facts: {stdout}"));
    assert_eq!(replayed, recorded);
}
