//! hdr-14.8: gate 5's mixed resource witness on `header-sync/storage`, the
//! one history where all four `Sim` resource rules run together:
//! `chunk-budget-bound`, `teardown-returns-baseline`,
//! `stale-completion-inert` and `outstanding-work-bounded`.
//!
//! The scenario closes and relinks both mirrors, so each comes back on a
//! fresh connection and session. It holds the node's first prepare across
//! its round's retirement, holds the first apply's reply across an
//! invalidation, reconsiders, then shuts the node down and stops its
//! storage, which the harness offers only once the stop is done. Each of
//! the four rules must hold and apply, every other property must hold or
//! stay `NotApplicable` with the reason [`NOT_APPLICABLE`] lists, every
//! control step must be picked, and the node must settle at its reactor's
//! exit. The acceptance manifest counts the committed fixture's history once
//! per rule (`rows.rs`'s `MIXED_RESOURCES`).
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, path::PathBuf, process::Command, rc::Rc};

use support::{
    byte_inputs, inputs,
    storage_witnesses::{hold, mirror, step, NODE, SOURCE},
};
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::{
    check::{after_step, PropertyOutcome},
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, OUTSTANDING_WORK_BOUNDED, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            claim::SettlementEvidence,
            ends::{EndEvidence, EndFact, EnvironmentAction},
            policy::{Allow, OpKind, ReplyHold, Select, Trigger},
            session::{SessionEvidence, SessionFact},
            stale::StaleEvidence,
            Chain, ControlAction, Policy, Reset,
        },
        header_sync::{Dialer, Storage, StorageScenario, Weights},
        run_prepared, Subsystem,
    },
    MemorySink, NaReason, Plan, PlanInput, RunReport,
};

/// The mirror below the node, which the node dials and serves.
const SERVED: &str = "mirror-1";

/// The four resource rules, each of which must hold and apply.
const RESOURCES: [&str; 4] = [
    CHUNK_BUDGET_BOUND,
    TEARDOWN_RETURNS_BASELINE,
    STALE_COMPLETION_INERT,
    OUTSTANDING_WORK_BOUNDED,
];

/// The storage harness's properties that stay `NotApplicable` here, each
/// with its reason: no scenario of this harness declares a livelock
/// assumption or a recovery. `rows.rs`'s `MIXED_NA` lists the same.
const NOT_APPLICABLE: [(&str, NaReason); 2] = [
    ("sim/no-livelock", NaReason::Undeclared),
    (
        "header-sync/bounded-adversary-progress",
        NaReason::Undeclared,
    ),
];

/// The control steps the run must pick, in script order: its completion
/// condition, ending with the `Shutdown` and the `StopStorage`.
const PICKS: [&str; 8] = [
    "peer:mirror-0:close",
    "peer:mirror-0:relink",
    "peer:node:reset",
    "peer:node:reset",
    "peer:mirror-1:close",
    "peer:mirror-1:relink",
    "peer:node:shutdown",
    "peer:node:stop-storage",
];

/// The committed fixture's stem.
const STEM: &str = "header-resources-mixed";

/// The node at A2 syncing from `mirror-0` at A8, and serving `mirror-1` at
/// A1, which it dials.
///
/// - The node's first prepare is held until `mirror-0`'s second session is
///   admitted, and `mirror-0`'s link closes once that prepare is submitted
///   and relinks once closed. The prepare's round retires with its session,
///   so it ends stale.
/// - The first apply's reply is held until the node resets. The node
///   invalidates its tip once that apply commits, and reconsiders once its
///   reply is delivered.
/// - `mirror-1`'s link then closes and relinks, so the node's serving
///   session churns too.
/// - The node shuts down once it admits `mirror-1`'s second session, and
///   its storage stops after that.
fn mixed_resources() -> StorageScenario {
    let first = |kind| Select::nth(kind, 1);
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            mirror(Chain::main(8), Dialer::Mirror),
            mirror(Chain::main(1), Dialer::Node),
        ],
        policy: Policy {
            holds: vec![hold(
                first(OpKind::Prepare),
                Trigger::Count(2, Box::new(Trigger::SessionAdmitted(SOURCE.to_owned()))),
                Allow::Engine,
            )],
            reply_holds: vec![ReplyHold {
                select: first(OpKind::Apply),
                until: Trigger::Reset(NODE.to_owned()),
            }],
            ..Policy::converging()
        },
        controls: vec![
            step(
                SOURCE,
                ControlAction::Close,
                Some(Trigger::Submitted(first(OpKind::Prepare))),
            ),
            step(
                SOURCE,
                ControlAction::Relink,
                Some(Trigger::Closed(SOURCE.to_owned())),
            ),
            step(
                NODE,
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::Committed(first(OpKind::Apply))),
            ),
            step(
                NODE,
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Completed(first(OpKind::Apply))),
            ),
            step(SERVED, ControlAction::Close, None),
            step(
                SERVED,
                ControlAction::Relink,
                Some(Trigger::Closed(SERVED.to_owned())),
            ),
            step(
                NODE,
                ControlAction::Shutdown,
                Some(Trigger::Count(
                    2,
                    Box::new(Trigger::SessionAdmitted(SERVED.to_owned())),
                )),
            ),
            step(NODE, ControlAction::StopStorage, None),
        ],
        weights: Weights::default(),
    }
}

/// One run of the scenario and the evidence its node left.
struct Run {
    report: RunReport,
    /// Every control step picked, by identity, in order.
    picks: Vec<String>,
    rounds: SettlementEvidence,
    stale: StaleEvidence,
    sessions: SessionEvidence,
    ends: EndEvidence,
}

/// Runs `scenario` under `input`, which must end with no violation and no
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Run {
    run_into(scenario, input, &mut MemorySink::default())
}

/// [`run`], recording into `sink`.
fn run_into(
    scenario: &StorageScenario,
    input: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> Run {
    let ports = Storage::prepare(scenario);
    let (rounds, stale, sessions, ends) = (
        ports.rounds.clone(),
        ports.stale_calls.clone(),
        ports.sessions.clone(),
        ports.ends.clone(),
    );
    let picks = Rc::new(RefCell::new(Vec::new()));
    let into = picks.clone();
    let mut checker = after_step(move |cx| {
        let pick = cx.pick.to_string();
        if pick.starts_with("peer:") {
            into.borrow_mut().push(pick);
        }
        None
    });
    let report = run_prepared::<Storage>(scenario, ports, input, sink, Some(&mut checker))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        (&report.end.violation, &report.end.harness),
        (&None, &None),
        "{:?}",
        report.end
    );
    let picks = picks.take();
    Run {
        report,
        picks,
        rounds,
        stale,
        sessions,
        ends,
    }
}

/// Checks `run` is the mixed witness: every control step picked in order,
/// each resource rule held and applied, every other property held or not
/// applicable as [`NOT_APPLICABLE`] lists, a prepare ended stale and a
/// round released, both mirrors back on fresh identities, and the node
/// stopped by the harness and settled at its reactor's exit.
fn witnessed(label: &str, run: &Run) {
    assert_eq!(run.picks, PICKS, "{label}");
    for property in &run.report.properties {
        let id = property.property.as_str();
        let listed = NOT_APPLICABLE.iter().find(|(na, _)| *na == id);
        match (&property.outcome, listed) {
            (PropertyOutcome::Held { applicable }, None) if *applicable > 0 => {}
            (PropertyOutcome::NotApplicable { reason }, Some((_, expected)))
                if reason == expected => {}
            (outcome, _) => panic!("{label}: {id}: {outcome:?}"),
        }
    }
    for id in RESOURCES
        .into_iter()
        .chain(NOT_APPLICABLE.map(|(id, _)| id))
    {
        assert!(
            run.report.properties.iter().any(|p| p.property == id),
            "{label}: {id} reports"
        );
    }
    assert!(
        run.stale
            .windows()
            .iter()
            .any(|window| !window.apply && window.step.is_some()),
        "{label}: a prepare ends stale: {:?}",
        run.stale.windows()
    );
    assert!(run.rounds.any_released(), "{label}: a round is released");
    assert!(
        run.rounds.settled(),
        "{label}: the stopped node settles: {:?}",
        run.rounds.rows()
    );
    assert_eq!(
        run.ends.reactor_exited(),
        None,
        "{label}: the harness stopped it"
    );
    assert!(
        run.ends.facts().iter().any(|(_, fact)| matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        )),
        "{label}: the harness's Shutdown is recorded"
    );
    let facts = run.sessions.facts();
    for peer in [SOURCE, SERVED] {
        let links: Vec<(u64, u64)> = facts
            .iter()
            .filter_map(|fact| match fact {
                SessionFact::LinkOpened {
                    peer: at,
                    connection,
                    session_id,
                    ..
                } if at == peer => Some((*connection, *session_id)),
                _ => None,
            })
            .collect();
        let admitted: Vec<u64> = facts
            .iter()
            .filter_map(|fact| match fact {
                SessionFact::SessionAdmitted {
                    peer: at,
                    session_id,
                    ..
                } if at == peer => Some(*session_id),
                _ => None,
            })
            .collect();
        let [(old_connection, old_session), (new_connection, new_session)] = links[..] else {
            panic!("{label}: {peer} links twice: {links:?}");
        };
        assert_ne!(old_connection, new_connection, "{label}: {peer}");
        assert_ne!(old_session, new_session, "{label}: {peer}");
        assert_eq!(admitted, [old_session, new_session], "{label}: {peer}");
    }
}

/// The Plan bytes the committed fixture runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture file `<STEM>.<extension>`.
fn fixture_file(extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{STEM}.{extension}"))
}

/// The committed fixture is [`mixed_resources`] as JSON, with the fixed Plan
/// bytes, and is the mixed witness under them.
#[test]
fn the_mixed_fixture_holds_every_resource_rule() {
    let committed: StorageScenario = serde_json::from_str(
        &fs::read_to_string(fixture_file("json")).expect("the fixture is committed"),
    )
    .expect("the fixture is a storage scenario");
    assert_eq!(
        committed,
        mixed_resources(),
        "run `regenerate_mixed_resources_fixture`"
    );
    let plan = fs::read(fixture_file("plan")).expect("the Plan is committed");
    assert_eq!(
        plan,
        committed_plan(),
        "run `regenerate_mixed_resources_fixture`"
    );
    // The run is the manifest history `STEM`, which the acceptance tool
    // records from this declaration (C18).
    let ran = run_declared(
        Storage::NAME,
        STEM,
        &committed,
        Plan::from_fuzz(&plan),
        run_into,
    );
    witnessed(STEM, &ran);
}

/// The scenario is the mixed witness under generated plans too, so no
/// honest schedule breaks a rule or skips a step.
#[test]
fn the_mixed_scenario_holds_under_generated_plans() {
    let scenario = mixed_resources();
    for (index, input) in inputs(8, 256).into_iter().enumerate() {
        witnessed(&format!("plan {index}"), &run(&scenario, input));
    }
}

/// Rewrites the committed fixture from [`mixed_resources`].
#[test]
#[ignore = "rewrites the committed mixed resource fixture"]
fn regenerate_mixed_resources_fixture() {
    let json = serde_json::to_string_pretty(&mixed_resources()).expect("scenarios serialize");
    fs::write(fixture_file("json"), json + "\n").expect("the fixture directory exists");
    fs::write(fixture_file("plan"), committed_plan()).expect("the fixture directory exists");
}

/// `record --witness` judges the committed fixture for each resource rule as
/// `cargo xtask planted` runs it, expecting a hold, and `replay` reruns each
/// trace Exact in a fresh process.
#[test]
fn the_mixed_fixture_records_and_replays_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    let scenario = fs::read_to_string(fixture_file("json")).expect("the fixture is committed");
    let plan = fixture_file("plan");
    for property in RESOURCES {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &scenario,
            "--plan",
            plan.to_str().expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            property,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{property}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{property}: {witness}"
        );
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{property}: {replayed:?}"
        );
    }
}
