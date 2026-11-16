//! hdr-4's fault-attribution wire witness on `header-sync/wire`: a peer
//! sends a page that passes the node's wire checks and fails preparation.
//!
//! The page is judged when the node's storage runs its preparation (ask 4,
//! option (b)), so `bad-content-closes` owes a consequence from then on and
//! `conformant-never-closes` accepts the node's punishment. The penalty rule
//! then wants a `Misbehavior` against the work's session, also when the peer
//! disconnected while the node was preparing the page. That disconnect
//! retires the round, so the failed prepare ends stale, and
//! `header-sync/stale-completion-inert` holds through its penalty.
#![cfg(zakura_sim)]

mod support;

use std::process::Command;

use support::{
    inputs,
    prepare_fault::{disconnect_mid_prepare, scenario, LastView, Uncooled, Watched, CHAIN},
    steer::steer,
};
use zakura_sim::{
    domain::header_sync::STALE_COMPLETION_INERT,
    harness::{
        header_node::{
            fault::FaultEnd,
            stale::{StaleWindow, Staleness},
        },
        header_sync::{StorageView, WirePorts, WireScenario, HEADER_FAULT_PENALIZED},
        run_prepared, HarnessError, Subsystem,
    },
    model::Label,
    observe::{ReactorFact, StorageFact, StorageRequest, StorageResult},
    peer::{honest::HonestChain, PeerAction},
    property::HasView,
    Checker, MemorySink, NoChecks, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};
use zakura_storage::header::fixture::HeaderFixture;

/// Runs `scenario` under `plan` with `extra` beside the registered
/// properties, keeping a run that ended in a harness failure. Returns the
/// report, its records and the last view its checks read.
fn run(
    scenario: &zakura_sim::harness::header_sync::WireScenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> ((RunReport, Vec<String>), StorageView) {
    run_as::<Watched>(scenario, plan, extra)
}

/// [`run`] under the harness `W`, [`Watched`] or [`Uncooled`].
fn run_as<W>(
    scenario: &zakura_sim::harness::header_sync::WireScenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> ((RunReport, Vec<String>), StorageView)
where
    W: Subsystem<Scenario = WireScenario, Prepared = (WirePorts, LastView)>
        + HasView<View = StorageView>,
{
    let (ports, last): (_, LastView) = W::prepare(scenario);
    let mut sink = MemorySink::default();
    let report = match run_prepared::<W>(
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
    ((report, sink.lines()), view)
}

/// The default plan, then byte-chosen ones.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(5, 16_384));
    plans
}

/// The invalid tip's hash as the log writes it.
fn invalid_tip() -> String {
    let tip = HonestChain::with_invalid_tip(&HeaderFixture::regtest(), CHAIN)
        .invalid_tip()
        .expect("the chain records its invalid tip");
    tip.hash
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// What a run showed about the invalid tip.
#[derive(Debug, Default)]
struct Shape {
    /// The operation that prepared the invalid tip, its session and where
    /// it completed in the log.
    prepare: Option<(u64, u64, usize)>,
    /// The prepare's result.
    result: Option<StorageResult>,
    /// Each `Misbehavior`: its origin and current session.
    misbehavior: Vec<(Option<u64>, Option<u64>)>,
    /// Where each `FrameJudged` of a `BadContent` page sits in the log.
    judged_bad: Vec<usize>,
}

/// Reads `report`'s shape.
fn shape(report: &RunReport) -> Shape {
    let tip = invalid_tip();
    let entries = report.log.entries();
    let mut shape = Shape::default();
    let mut submitted = None;
    let mut bad = Vec::new();
    for (at, entry) in entries.iter().enumerate() {
        match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { owner, target, .. },
                        ..
                    },
                ..
            } if target.hash == tip => submitted = Some((*id, owner.session_id)),
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Completed { id, result, .. },
                ..
            } if submitted.is_some_and(|(op, _)| op == *id) && shape.prepare.is_none() => {
                let (op, session) = submitted.expect("matched above");
                shape.prepare = Some((op, session, at));
                shape.result = Some(result.clone());
            }
            Observation::Reactor {
                event:
                    ReactorFact::Misbehavior {
                        origin, current, ..
                    },
                ..
            } => shape.misbehavior.push((*origin, *current)),
            Observation::PeerFrame {
                connection,
                quic_stream,
                seq,
                label,
                ..
            } if *label == Label::BadContent => {
                bad.push((*connection, *quic_stream, *seq));
            }
            Observation::FrameJudged {
                connection,
                quic_stream,
                seq,
                ..
            } if bad.contains(&(*connection, *quic_stream, *seq)) => shape.judged_bad.push(at),
            _ => {}
        }
    }
    shape
}

/// Asserts that `report` passed every registered property and the penalty
/// rule, prepared the invalid tip once with an `InvalidHeader`, judged its
/// page only after that, and penalized the work's session.
fn assert_penalized(label: &str, report: &RunReport, view: &StorageView) -> Shape {
    assert!(
        report.end.violation.is_none() && report.end.harness.is_none(),
        "{label}: {:?}",
        report.end
    );
    assert_eq!(
        view.faults.penalized(report.log.entries()),
        FaultEnd::default(),
        "{label}"
    );
    let shape = shape(report);
    let (_, session, completed) = shape.prepare.expect("the node prepared the invalid tip");
    assert_eq!(
        shape.result,
        Some(StorageResult::PrepareFailed {
            category: "invalid_header".to_owned()
        }),
        "{label}"
    );
    assert!(
        !shape.judged_bad.is_empty() && shape.judged_bad.iter().all(|at| *at > completed),
        "{label}: the page is judged once its preparation ran: {shape:?}"
    );
    assert!(
        shape
            .misbehavior
            .iter()
            .any(|(origin, _)| *origin == Some(session)),
        "{label}: {shape:?}"
    );
    shape
}

/// The node prepares the invalid tip, penalizes the peer and closes it. The
/// close isn't one `conformant-never-closes` flags, since the page was judged
/// at its preparation.
#[test]
fn an_invalid_page_is_judged_at_prepare_and_penalized() {
    for (index, plan) in plans().into_iter().enumerate() {
        let ((report, _), view) = run(&scenario(Vec::new(), false), plan, &mut NoChecks);
        let shape = assert_penalized(&format!("plan {index}"), &report, &view);
        assert!(
            shape
                .misbehavior
                .iter()
                .all(|(origin, current)| origin == current),
            "plan {index}: the session that sent the page was still current: {shape:?}"
        );
    }
}

/// The peer disconnects while the node prepares its invalid tip. The prepare
/// fails after the session ended, and the node still penalizes the session
/// that sent the page. That prepare is a stale peer fault, and
/// [`assert_stale_fault_inert`] checks it.
#[test]
fn a_disconnect_mid_prepare_is_still_penalized() {
    for (index, plan) in plans().into_iter().enumerate() {
        let scenario = scenario(vec![PeerAction::Disconnect], true);
        let mut last = None;
        let (report, _, _) = steer(plan, &disconnect_mid_prepare, &mut |plan, checker| {
            let (run, view) = run(&scenario, plan, checker);
            last = Some(view);
            run
        });
        let view = last.expect("the steered run's view");
        let shape = assert_penalized(&format!("plan {index}"), &report, &view);
        let (_, session, _) = shape.prepare.expect("checked above");
        assert!(
            shape.misbehavior.contains(&(Some(session), None)),
            "plan {index}: the penalty came after the session ended: {shape:?}"
        );
        assert_stale_fault_inert(index, &report, &view, &shape);
    }
}

/// Checks the stale peer fault in a [`disconnect_mid_prepare`] run, end
/// to end: the disconnect retired the round, so the prepare that failed
/// `invalid_header` ended stale, the only stale call. Its handler reported
/// the `Misbehavior`, between the call's `CallEnded` and its `Handled`,
/// which `header-sync/stale-completion-inert` allows, and the property
/// applied and held.
fn assert_stale_fault_inert(index: usize, report: &RunReport, view: &StorageView, shape: &Shape) {
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == STALE_COMPLETION_INERT)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "plan {index}: {outcome:?}"
    );

    let (op, _, _) = shape.prepare.expect("checked above");
    let facts: Vec<&ReactorFact> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { event, .. } => Some(event),
            _ => None,
        })
        .collect();
    let call = facts
        .iter()
        .find_map(|fact| match fact {
            ReactorFact::CallEnded {
                call,
                op: Some(ended),
                ..
            } if *ended == op => Some(*call),
            _ => None,
        })
        .unwrap_or_else(|| panic!("plan {index}: the invalid tip's prepare ends"));
    let windows = view.stale_calls.windows();
    let [StaleWindow {
        call: stale,
        apply: false,
        why: Staleness::RoundRetired,
        handled: true,
        ..
    }] = windows[..]
    else {
        panic!("plan {index}: only the invalid tip's prepare ends stale: {windows:?}");
    };
    assert_eq!(stale, call, "plan {index}");
    let at = |found: &dyn Fn(&ReactorFact) -> bool| {
        facts
            .iter()
            .position(|fact| found(fact))
            .unwrap_or_else(|| panic!("plan {index}: {facts:?}"))
    };
    let ended = at(&|fact| matches!(fact, ReactorFact::CallEnded { call: c, .. } if *c == call));
    let handled = at(&|fact| matches!(fact, ReactorFact::Handled { call: c, .. } if *c == call));
    let penalty = at(&|fact| matches!(fact, ReactorFact::Misbehavior { .. }));
    assert!(
        ended < penalty && penalty < handled,
        "plan {index}: the stale handler reports the penalty: {ended} {penalty} {handled}"
    );
}

/// The penalty rule's planted bypass: the same disconnect, judged by a view
/// that never sees the node's cooldowns, as a node that installs none would
/// show. `header-sync/header-fault-penalized` ends the run as
/// `UncooledFault` once the reactor has handled the failed preparation.
#[test]
fn hidden_cooldowns_violate_the_penalty_rule() {
    let scenario = scenario(vec![PeerAction::Disconnect], true);
    let (report, _, _) = steer(
        plans().swap_remove(0),
        &disconnect_mid_prepare,
        &mut |plan, checker| run_as::<Uncooled>(&scenario, plan, checker).0,
    );
    let violation = report
        .end
        .violation
        .as_ref()
        .unwrap_or_else(|| panic!("the hidden cooldown is a violation: {:?}", report.end));
    assert_eq!(violation.property, HEADER_FAULT_PENALIZED);
    assert!(
        violation.actual.starts_with("UncooledFault"),
        "{violation:?}"
    );
    assert!(report.end.harness.is_none(), "{:?}", report.end);
}

/// The witness's evidence under `plan`: whom each storage failure blamed and
/// the run's log, as text.
fn evidence(plan: PlanInput) -> (String, String) {
    let ((report, _), view) = run(&scenario(Vec::new(), false), plan, &mut NoChecks);
    (
        format!("{:?}", view.faults.attributions().all()),
        format!("{:?}", report.log.entries()),
    )
}

/// The evidence depends only on scenario and plan: plan A twice, with a
/// plan B between whose evidence differs, gives A's both times.
#[test]
fn the_evidence_repeats_a_a_and_a_b_a() {
    let plans = plans();
    let first = evidence(plans[0].clone());
    let again = evidence(plans[0].clone());
    let other = plans[1..]
        .iter()
        .find(|plan| evidence((*plan).clone()) != first)
        .expect("some plan gives different evidence");
    let _ = evidence(other.clone());
    let last = evidence(plans[0].clone());
    assert!(first == again && again == last, "plan A, then B, then A");
}

/// The environment variable that makes [`evidence_for_the_parent`] print the
/// witness's evidence under the plan it names.
const PLAN_INDEX: &str = "ZAKURA_SIM_PREPARE_FAULT_PLAN";

/// Prints the witness's evidence under the plan `PLAN_INDEX` names, for
/// [`the_evidence_replays_in_a_fresh_process`]. Does nothing without it.
#[test]
fn evidence_for_the_parent() {
    let Ok(index) = std::env::var(PLAN_INDEX) else {
        return;
    };
    let index: usize = index.parse().expect("the parent passes a plan index");
    let (attributions, log) = evidence(plans().swap_remove(index));
    #[allow(clippy::print_stdout)]
    {
        println!("ATTRIBUTIONS {attributions}");
        println!("LOG {log}");
    }
}

/// A fresh process running the witness under the same plan gives the same
/// evidence.
#[test]
fn the_evidence_replays_in_a_fresh_process() {
    let index = 2;
    let (attributions, log) = evidence(plans().swap_remove(index));
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args(["--exact", "evidence_for_the_parent", "--nocapture"])
        .env(PLAN_INDEX, index.to_string())
        .output()
        .expect("the test binary runs");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = |prefix: &str| {
        stdout
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .unwrap_or_else(|| panic!("the child printed {prefix}: {stdout}"))
            .to_owned()
    };
    assert_eq!(line("ATTRIBUTIONS "), attributions);
    assert_eq!(line("LOG "), log);
}
