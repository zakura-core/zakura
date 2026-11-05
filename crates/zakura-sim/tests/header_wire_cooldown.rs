//! hdr-5's readmission cooldown witness on `header-sync/wire`: a peer sends a
//! page that passes the node's wire checks and fails preparation, disconnects
//! while the node prepares it, and dials again.
//!
//! The page is judged at its preparation (ask 4, option (b)). The failed
//! preparation reports a `Misbehavior` against the departed session, then the
//! `CooldownStarted` it installs. The redial comes within the cooldown, and
//! the header service parks its session for the back-off the reactor
//! published, so the reactor never sees it, let alone admits it. A reactor
//! that sees a reconnect in the cooldown refuses it with `SessionRefused`,
//! which the reactor's unit witness shows, since a direct handover is the
//! only way to reach that check. `header-sync/header-fault-penalized` holds
//! over the run, and own-9 retires the departed session.
#![cfg(zakura_sim)]

mod support;

use std::{process::Command, time::Duration};

use zakura_network::zakura::ZAKURA_STREAM_HEADER_SYNC;

use support::{
    inputs,
    prepare_fault::{redial_after_disconnect, scenario, LastView, Watched},
    steer::steer,
};
use zakura_sim::{
    harness::{
        header_node::{
            adoption::Acceptance,
            fault::CooldownFact,
            session::{EndCheck, SessionEvidence, SessionFact},
        },
        header_sync::{StorageView, WireScenario, HEADER_FAULT_PENALIZED},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{ReactorFact, SessionStepFact, StorageFact, StorageRequest, TransportFact},
    peer::PeerAction,
    Checker, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The readmission cooldown the node's default config sets.
const COOLDOWN: Duration = Duration::from_secs(60);

/// What one steered run gives: its report, the last view its checks read,
/// the node's session view, and the steered plan.
struct Steered {
    report: RunReport,
    view: StorageView,
    sessions: SessionEvidence,
    plan: PlanInput,
}

/// Runs `scenario` under `plan` with `extra` beside the registered
/// properties, keeping a run that ended in a harness failure. Returns the
/// report, its records, the last view its checks read and the session view.
fn run(
    scenario: &WireScenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> ((RunReport, Vec<String>), StorageView, SessionEvidence) {
    let (ports, last): (_, LastView) = Watched::prepare(scenario);
    let sessions = ports.sessions.clone();
    let mut sink = MemorySink::default();
    let report = match run_prepared::<Watched>(
        scenario,
        (ports, last.clone()),
        plan,
        &mut sink,
        Some(extra),
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    let view = last.borrow().clone().expect("the run's checks read a view");
    ((report, sink.lines()), view, sessions)
}

/// Steers `plan` so the peer disconnects while the node prepares its invalid
/// tip and redials as soon as it can.
fn steered(plan: PlanInput) -> Steered {
    let scenario = scenario(vec![PeerAction::Disconnect, PeerAction::Redial], true);
    let mut last = None;
    let (report, plan, _) = steer(plan, &redial_after_disconnect, &mut |plan, checker| {
        let (run, view, sessions) = run(&scenario, plan, checker);
        last = Some((view, sessions));
        run
    });
    let (view, sessions) = last.expect("the steered run's view");
    Steered {
        report,
        view,
        sessions,
        plan,
    }
}

/// The default plan, then byte-chosen ones.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(5, 16_384));
    plans
}

/// The session that sent the invalid page, from the node's one failed
/// preparation, and the `Misbehavior` that penalized it: its peer, its
/// reactor event number and its current session.
fn penalized(report: &RunReport) -> (u64, String, u64, Option<u64>) {
    let entries = report.log.entries();
    let failed: Vec<(u64, u64)> = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { owner, .. },
                        ..
                    },
                ..
            } => Some((*id, owner.session_id)),
            _ => None,
        })
        .collect();
    let [(_, session)] = failed[..] else {
        panic!("the node prepares once: {failed:?}");
    };
    let misbehavior: Vec<(String, u64, Option<u64>)> = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                seq,
                event:
                    ReactorFact::Misbehavior {
                        peer,
                        origin: Some(origin),
                        current,
                        ..
                    },
                ..
            } if *origin == session => Some((peer.clone(), *seq, *current)),
            _ => None,
        })
        .collect();
    let [(peer, seq, current)] = &misbehavior[..] else {
        panic!("one Misbehavior names session {session}: {misbehavior:?}");
    };
    (session, peer.clone(), *seq, *current)
}

/// The peer leaves while the node prepares its invalid tip, and its redial
/// comes within the cooldown that the late fault started. The reactor
/// penalizes the departed session and reports the cooldown right after. The
/// redial reaches own-9 as a new header session, and the service parks it
/// for the back-off the reactor published, so the reactor admits nothing
/// from the peer. Every registered property holds,
/// `header-fault-penalized` among them, and own-9 retires the departed
/// session.
#[test]
fn a_late_fault_cools_down_the_peer_and_refuses_its_redial() {
    for (index, plan) in plans().into_iter().enumerate() {
        let Steered {
            report,
            view,
            sessions,
            ..
        } = steered(plan);
        assert!(
            report.end.violation.is_none() && report.end.harness.is_none(),
            "plan {index}: {:?}",
            report.end
        );
        let outcome = report
            .properties
            .iter()
            .find(|property| property.property == HEADER_FAULT_PENALIZED)
            .map(|property| property.outcome.clone());
        assert!(
            matches!(outcome, Some(PropertyOutcome::Held { .. })),
            "plan {index}: {outcome:?}"
        );
        assert_eq!(
            view.faults.penalized(report.log.entries()),
            Acceptance::Holds,
            "plan {index}"
        );

        let (session, peer, seq, current) = penalized(&report);
        assert_eq!(current, None, "plan {index}: the session had left");
        let facts = view.faults.cooldowns().facts();
        let started = facts
            .iter()
            .position(|fact| {
                matches!(fact, CooldownFact::Started { peer: cooled, origin, cooldown, after, .. }
                    if *cooled == peer && *origin == session && *cooldown == COOLDOWN && *after == seq)
            })
            .unwrap_or_else(|| panic!("plan {index}: no cooldown after the penalty: {facts:?}"));
        assert!(
            !facts[started..].iter().any(
                |fact| matches!(fact, CooldownFact::Admitted { peer: admitted, .. } if *admitted == peer)
            ),
            "plan {index}: nothing from the peer is admitted: {facts:?}"
        );

        // The redial reaches own-9 as a new header session, which the
        // service parks for the published back-off, so the reactor never
        // admits it.
        let redial = redialed(&report);
        assert!(
            !sessions.facts().iter().any(|fact| matches!(
                fact,
                SessionFact::SessionAdmitted { session_id, .. } if *session_id == redial
            )),
            "plan {index}: {:?}",
            sessions.facts()
        );
        assert_eq!(sessions.end_check(), EndCheck::Complete, "plan {index}");
    }
}

/// The session ID of the redial's header stream: own-9's header `Session`
/// row on the peer's second connection, whose generation own-9 parked and
/// never installed.
fn redialed(report: &RunReport) -> u64 {
    let entries = report.log.entries();
    let rows: Vec<(u64, u64)> = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Session {
                connection,
                peer,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                session_id,
                ..
            } if peer == "peer-0" => Some((*connection, *session_id)),
            _ => None,
        })
        .collect();
    let [_, (connection, session_id)] = rows[..] else {
        panic!("the peer opens one header session per connection: {rows:?}");
    };
    let generation = |parked: bool| {
        entries.iter().any(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(on),
                event: TransportFact::SessionLifecycle { step, .. },
                ..
            } if *on == connection => {
                if parked {
                    matches!(step, SessionStepFact::GenerationParked { generation } if *generation == session_id)
                } else {
                    matches!(step, SessionStepFact::GenerationInstalled { generation, .. } if *generation == session_id)
                }
            }
            _ => false,
        })
    };
    assert!(
        generation(true) && !generation(false),
        "own-9 parks the redial's generation and never installs it"
    );
    session_id
}

/// The witness's evidence under the steered `plan`: the cooldown facts, the
/// session view's facts and end check, and the run's log, as text.
fn evidence(plan: PlanInput) -> String {
    let Steered {
        report,
        view,
        sessions,
        ..
    } = steered(plan);
    format!(
        "{:?}\n{:?}\n{:?}\n{:?}",
        view.faults.cooldowns().facts(),
        sessions.facts(),
        sessions.end_check(),
        report.log.entries()
    )
}

/// The evidence depends only on scenario and plan: plan A twice, with a
/// plan B between whose evidence differs, gives A's both times. Each runs
/// the plan steering found for it.
#[test]
fn the_evidence_repeats_a_a_and_a_b_a() {
    let plans = plans();
    let a = steered(plans[0].clone()).plan;
    let b = steered(plans[1].clone()).plan;
    let replay = |plan: &PlanInput| {
        let scenario = scenario(vec![PeerAction::Disconnect, PeerAction::Redial], true);
        let ((report, _), view, sessions) = run(&scenario, plan.clone(), &mut zakura_sim::NoChecks);
        format!(
            "{:?}\n{:?}\n{:?}\n{:?}",
            view.faults.cooldowns().facts(),
            sessions.facts(),
            sessions.end_check(),
            report.log.entries()
        )
    };
    let first = replay(&a);
    let again = replay(&a);
    let other = replay(&b);
    let last = replay(&a);
    assert_ne!(first, other, "plan B gives different evidence");
    assert!(first == again && again == last, "plan A, then B, then A");
    assert_eq!(first, evidence(plans[0].clone()), "steering gives plan A");
}

/// The environment variable that makes [`evidence_for_the_parent`] print the
/// witness's evidence under the plan it names.
const PLAN_INDEX: &str = "ZAKURA_SIM_COOLDOWN_PLAN";

/// Prints the witness's evidence under the plan `PLAN_INDEX` names, for
/// [`the_evidence_replays_in_a_fresh_process`]. Does nothing without it.
#[test]
fn evidence_for_the_parent() {
    let Ok(index) = std::env::var(PLAN_INDEX) else {
        return;
    };
    let index: usize = index.parse().expect("the parent passes a plan index");
    let evidence = evidence(plans().swap_remove(index));
    #[allow(clippy::print_stdout)]
    {
        println!("EVIDENCE {}", evidence.replace('\n', " | "));
    }
}

/// A fresh process steering and running the witness under the same plan
/// gives the same evidence.
#[test]
fn the_evidence_replays_in_a_fresh_process() {
    let index = 2;
    let evidence = evidence(plans().swap_remove(index)).replace('\n', " | ");
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args(["--exact", "evidence_for_the_parent", "--nocapture"])
        .env(PLAN_INDEX, index.to_string())
        .output()
        .expect("the test binary runs");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let printed = stdout
        .lines()
        .find_map(|line| line.strip_prefix("EVIDENCE "))
        .unwrap_or_else(|| panic!("the child printed its evidence: {stdout}"));
    assert_eq!(printed, evidence);
}
