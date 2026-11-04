//! hdr-5's session view on `header-sync/storage`: the reactor's session
//! admissions and ends, joined to the links the harness made. Each run also
//! checks that no work starts or applies under a session that left the
//! reactor's view.
#![cfg(zakura_sim)]

mod support;

use std::{io::Write, process::Command};

use support::inputs;
use zakura_network::zakura::{ServicePeerDirection, SessionOrigin};
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            session::SessionFact,
            ControlAction, ControlStep, Policy,
        },
        header_sync::{Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure. Returns its report and the node's
/// session facts.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Vec<SessionFact>) {
    let ports = Storage::prepare(scenario);
    let sessions = ports.sessions.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    (report, sessions.facts())
}

/// `action` on `mirror-0`, offered once `when` holds.
fn step(action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: "mirror-0".to_owned(),
        action,
        when: Some(when),
    }
}

/// The node's `index`th admission, from 0.
fn admission(facts: &[SessionFact], index: usize) -> &SessionFact {
    facts
        .iter()
        .filter(|fact| matches!(fact, SessionFact::SessionAdmitted { .. }))
        .nth(index)
        .unwrap_or_else(|| panic!("the node admits session {index}: {facts:#?}"))
}

/// The admission of `mirror-0`'s session `id`, inbound on the link with the
/// same connection and generation, replacing `replaced`.
fn admitted(id: u64, replaced: Option<u64>) -> SessionFact {
    SessionFact::SessionAdmitted {
        peer: "mirror-0".to_owned(),
        session_id: id,
        direction: ServicePeerDirection::Inbound,
        replaced,
        origin: Some(SessionOrigin {
            connection: id,
            quic_stream: None,
            generation: id,
        }),
    }
}

/// The harness's link to `mirror-0` on connection `id`, which it dials.
fn opened(id: u64) -> SessionFact {
    SessionFact::LinkOpened {
        peer: "mirror-0".to_owned(),
        connection: id,
        session_id: id,
        dialer: "mirror-0".to_owned(),
    }
}

/// Where `fact` is in `facts`.
fn position(facts: &[SessionFact], fact: &SessionFact) -> Option<usize> {
    facts.iter().position(|each| each == fact)
}

/// The close witness: `mirror-0`'s link closes once the node reserves
/// headers for it, and it relinks once the close is made.
fn close_witness() -> StorageScenario {
    StorageScenario {
        controls: vec![
            step(
                ControlAction::Close,
                Trigger::ReservationOpened("mirror-0".to_owned()),
            ),
            step(
                ControlAction::Relink,
                Trigger::Closed("mirror-0".to_owned()),
            ),
        ],
        ..StorageScenario::default()
    }
}

/// The relink witness: the node's first prepare is held until the node
/// admits `mirror-0`'s second session, and `mirror-0` relinks, without
/// closing its first link, once that prepare is submitted.
fn relink_witness() -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted("mirror-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![step(
            ControlAction::Relink,
            Trigger::Submitted(Select::nth(OpKind::Prepare, 1)),
        )],
        ..StorageScenario::default()
    }
}

/// A close then a relink: each link's session is admitted with that link's
/// origin and session ID, and the first session leaves the reactor's view
/// exactly once. Its disconnect ends it, or, when the relink is admitted
/// first, the relink replaces it and the disconnect is a no-op. Each way
/// happens under some plan.
#[test]
fn a_close_then_a_relink_ends_or_replaces_the_first_session() {
    let scenario = close_witness();
    let (mut ended, mut replaced) = (0, 0);
    for input in inputs(32, 256) {
        let (_, facts) = run(&scenario, input);
        assert_eq!(facts[..2], [opened(1), admitted(1, None)], "{facts:#?}");
        let closed = SessionFact::LinkClosed {
            peer: "mirror-0".to_owned(),
            connection: 1,
        };
        let closed = position(&facts, &closed).expect("the first link closes");
        let relinked = position(&facts, &opened(2)).expect("the mirror relinks");
        assert!(closed < relinked, "{facts:#?}");
        let end = facts
            .iter()
            .position(|fact| matches!(fact, SessionFact::SessionEnded { session_id: 1, .. }));
        match (end, admission(&facts, 1)) {
            (Some(end), second) if *second == admitted(2, None) => {
                assert!(
                    end < position(&facts, second).expect("it was found there"),
                    "{facts:#?}"
                );
                ended += 1;
            }
            (None, second) if *second == admitted(2, Some(1)) => replaced += 1,
            (end, second) => panic!("session 1 ended at {end:?}, then {second:?}: {facts:#?}"),
        }
    }
    assert!(
        ended > 0,
        "no plan ended the first session before the relink"
    );
    assert!(replaced > 0, "no plan admitted the relink first");
}

/// The node's first prepare's operation ID, if it prepared.
fn first_prepare(report: &RunReport) -> Option<u64> {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    },
                ..
            } => Some(*id),
            _ => None,
        })
}

/// Whether the node's storage completed operation `op`.
fn completed(report: &RunReport, op: u64) -> bool {
    report.log.entries().iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Completed { id, .. },
                ..
            } if *id == op
        )
    })
}

/// A relink without a close: the second link's admission replaces the first
/// session while the first link stays open, and releases the old session's
/// held prepare. The prepare still completes, and since the run passes, its
/// completion started and applied nothing under the replaced session.
#[test]
fn a_relink_replaces_the_session_and_its_held_prepare_stays_inert() {
    let scenario = relink_witness();
    for input in inputs(16, 256) {
        let (report, facts) = run(&scenario, input);
        assert_eq!(
            facts[..4],
            [
                opened(1),
                admitted(1, None),
                opened(2),
                admitted(2, Some(1))
            ],
            "{facts:#?}"
        );
        assert!(
            !facts
                .iter()
                .any(|fact| matches!(fact, SessionFact::SessionEnded { session_id: 1, .. })),
            "a replaced session's disconnect ends nothing: {facts:#?}"
        );
        let prepare = first_prepare(&report).expect("the node prepares");
        assert!(
            completed(&report, prepare),
            "the admission releases the prepare"
        );
    }
}

/// Both witnesses' session facts and full logs depend only on scenario and
/// plan: plan A twice, with plan B between, gives A's both times, and B's
/// differ.
#[test]
fn the_session_witnesses_repeat_a_a_and_a_b_a() {
    let plans = inputs(3, 256);
    for scenario in [close_witness(), relink_witness()] {
        let runs: Vec<(String, Vec<SessionFact>)> = [&plans[1], &plans[2], &plans[1]]
            .into_iter()
            .map(|input| {
                let (report, facts) = run(&scenario, input.clone());
                (format!("{:?}", report.log.entries()), facts)
            })
            .collect();
        assert_eq!(runs[0], runs[2], "plan A, then B, then A");
        assert_ne!(runs[0].0, runs[1].0, "plan B's log differs");
    }
}

/// Set in the child process that
/// [`the_session_witnesses_repeat_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_SESSION_FACTS_CHILD";

/// Both witnesses' session facts under one plan, as text.
fn witness_facts() -> String {
    let input = inputs(1, 256).remove(0);
    [close_witness(), relink_witness()]
        .iter()
        .map(|scenario| format!("{:?}\n", run(scenario, input.clone()).1))
        .collect()
}

/// A fresh process that runs both witnesses under the same plan gives the
/// same session facts as this one.
#[test]
fn the_session_witnesses_repeat_in_a_fresh_process() {
    let facts = witness_facts();
    if std::env::var_os(CHILD).is_some() {
        // The parent reads the facts from the child's stdout.
        writeln!(std::io::stdout(), "FACTS-BEGIN\n{facts}FACTS-END")
            .expect("the test harness captures stdout");
        return;
    }
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_session_witnesses_repeat_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("FACTS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("FACTS-END"))
        .map(|(facts, _)| facts)
        .unwrap_or_else(|| panic!("the child prints its facts: {stdout}"));
    assert_eq!(replayed, facts);
}
