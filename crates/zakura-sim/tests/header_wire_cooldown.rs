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

use std::{collections::BTreeMap, process::Command, time::Duration};

use zakura_network::zakura::{ZakuraHeaderSyncConfig, ZAKURA_STREAM_HEADER_SYNC};

use support::{
    inputs,
    prepare_fault::{redial_after_disconnect, scenario, LastView, Watched},
    steer::{steer, Pair},
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
    Checker, MemorySink, Observation, ObservationLog, Plan, PlanInput, PropertyOutcome,
    PropertyReport, RunEnd, RunReport, StepCtx, Violation,
};

/// The readmission cooldown the node's default config sets.
const COOLDOWN: Duration = Duration::from_secs(60);

/// What one steered run gives: its report, the last view its checks read,
/// the node's session view, and the steered plan.
struct Steered {
    report: RunReport,
    view: StorageView,
    sessions: SessionEvidence,
    times: StepTimes,
    plan: PlanInput,
}

/// The virtual time after each step since the run's start, which the log
/// itself doesn't carry.
#[derive(Default)]
struct StepTimes(BTreeMap<u64, Duration>);

impl StepTimes {
    /// The time after `step`.
    fn at(&self, step: u64) -> Duration {
        *self.0.get(&step).expect("every step's time is recorded")
    }
}

impl Checker for StepTimes {
    /// Records the step's time.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        self.0.insert(cx.step, cx.now.since_start());
        None
    }

    /// Nothing.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
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
        let mut times = StepTimes::default();
        let (run, view, sessions) = run(&scenario, plan, &mut Pair(checker, &mut times));
        last = Some((view, sessions, times));
        run
    });
    let (view, sessions, times) = last.expect("the steered run's view");
    Steered {
        report,
        view,
        sessions,
        times,
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
            times,
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
        let CooldownFact::Started { at: from, .. } = facts[started] else {
            unreachable!("matched above");
        };
        // Only the cooldown's window is checked here. Where it ends is
        // pinned by the reactor's unit witness,
        // `a_late_fault_refuses_reconnects_until_its_cooldown_ends`.
        assert!(
            !facts[started..].iter().any(|fact| matches!(fact,
                CooldownFact::Admitted { peer: admitted, at, .. }
                    if *admitted == peer && *at < from + COOLDOWN)),
            "plan {index}: nothing from the peer is admitted in the cooldown: {facts:?}"
        );

        // The redial reaches own-9 as a new header session inside the
        // cooldown, and the service parks it, so the reactor never admits
        // it. The back-off is what parks it: the peer's first connection
        // was torn down first, so no live session collides with it, and
        // the reactor held no session, so a peer slot was free.
        let redial = redialed(&report);
        let parked = times.at(redial.parked);
        assert!(
            from <= parked && parked < from + COOLDOWN,
            "plan {index}: parked at {parked:?}, cooldown from {from:?}"
        );
        assert!(
            redial.first_torn_down < redial.row,
            "plan {index}: {redial:?}"
        );
        // These pin this fixture's whole history, not the cooldown: the
        // parked redial's connection idles out before anything readmits
        // it, so own-9 only ever admits and ends the first session.
        let session_facts = sessions.facts();
        assert!(
            matches!(
                session_facts
                    .iter()
                    .filter(|fact| matches!(
                        fact,
                        SessionFact::SessionAdmitted { .. } | SessionFact::SessionEnded { .. }
                    ))
                    .collect::<Vec<_>>()[..],
                [
                    SessionFact::SessionAdmitted { session_id: admitted, .. },
                    SessionFact::SessionEnded { session_id: ended, .. },
                ] if admitted == ended && *admitted == session
            ),
            "plan {index}: {session_facts:?}"
        );
        assert!(
            ZakuraHeaderSyncConfig::default()
                .peer_limits
                .max_inbound_peers
                > 0
        );
        assert!(
            !session_facts.iter().any(|fact| matches!(
                fact,
                SessionFact::SessionAdmitted { session_id, .. } if *session_id == redial.session_id
            )),
            "plan {index}: {session_facts:?}"
        );
        assert_eq!(sessions.end_check(), EndCheck::Complete, "plan {index}");
    }
}

/// Where the redial's header session shows in a run's log.
#[derive(Debug)]
struct Redial {
    /// Its session ID, which own-9 also gave its generation.
    session_id: u64,
    /// The step that logged own-9 parking its generation.
    parked: u64,
    /// Where its `Session` row is in the log.
    row: usize,
    /// Where the peer's first connection's `torn-down` is in the log.
    first_torn_down: usize,
}

/// The redial's header session: own-9's header `Session` row on the peer's
/// second connection, whose generation own-9 parked, never installed, and
/// then let exit.
fn redialed(report: &RunReport) -> Redial {
    let entries = report.log.entries();
    let rows: Vec<(usize, u64, u64)> = entries
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Session {
                connection,
                peer,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                session_id,
                ..
            } if peer == "peer-0" => Some((at, *connection, *session_id)),
            _ => None,
        })
        .collect();
    let [(_, first, _), (row, connection, session_id)] = rows[..] else {
        panic!("the peer opens one header session per connection: {rows:?}");
    };
    let first_torn_down = entries
        .iter()
        .position(|entry| {
            matches!(
                &entry.obs,
                Observation::Transport {
                    connection: Some(on),
                    event: TransportFact::TornDown,
                    ..
                } if *on == first
            )
        })
        .expect("the peer's first connection is torn down");
    let steps: Vec<(u64, &SessionStepFact)> = entries
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(on),
                event:
                    TransportFact::SessionLifecycle {
                        kind: ZAKURA_STREAM_HEADER_SYNC,
                        step,
                    },
                ..
            } if *on == connection => Some((entry.step, step)),
            _ => None,
        })
        .filter(|(_, step)| match step {
            SessionStepFact::GenerationInstalled { generation, .. }
            | SessionStepFact::GenerationParked { generation }
            | SessionStepFact::GenerationExited { generation, .. } => *generation == session_id,
            _ => false,
        })
        .collect();
    let [(parked, SessionStepFact::GenerationParked { .. }), (_, SessionStepFact::GenerationExited { current: false, .. })] =
        steps[..]
    else {
        panic!(
            "own-9 parks the redial's generation, never installs it, and lets it exit: {steps:?}"
        );
    };
    Redial {
        session_id,
        parked,
        row,
        first_torn_down,
    }
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
