//! `header-sync/stale-completion-inert` through a VCT body repair, on
//! `header-sync/repair`. In the repairing fixtures, the node runs a repair
//! against a mirror that holds the repaired headers' tree records, and the
//! repair applies. When the
//! supplier leaves while the repair prepares or applies, the node keeps the
//! round, so its calls are current, not stale, and its apply still
//! succeeds. Each fixture also makes one ordinary stale prepare, so the
//! property applies and holds.
//!
//! `header-sync/outstanding-work-bounded` holds on every committed input
//! too. On the supplier-pruned input, whose supplier lacks the tree records
//! so the repair retires `HistoryPruned` and never applies, it judges the
//! repair requirement's source sets through a charge and a departure.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    fs,
    future::Future,
    io::Write,
    num::NonZeroU64,
    path::{Path, PathBuf},
    pin::pin,
    process::Command,
    task::{Context, Poll, Waker},
};

use serde_json::Value;
use tempfile::TempDir;
use zakura_chain::block;
use zakura_header_chain::BodyWorkAuthority;
use zakura_node_services::header_chain::{Port, VctRepairContextReply};
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::NaReason;
use zakura_sim::{
    domain::header_sync::{
        COMMIT_ONLY_AUTHORIZED, OUTSTANDING_WORK_BOUNDED, STALE_COMPLETION_INERT,
    },
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::outstanding::OutstandingEvidence,
        header_node::stale::{StaleEvidence, StaleWindow, Staleness},
        header_sync::{RepairHarness, RepairScenario, REPAIR_HEIGHT},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    observe::{Observation, ReactorCall, ReactorFact},
    record::RecordSink,
    seed,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, NullSink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};
use zakura_storage::header::controlled::ControlledPort;

/// The committed inputs, `tests/fixtures/header-repair-*.bin`: a fixture's
/// scenario byte, then Plan bytes.
const APPLIED_INPUT: &[u8] = include_bytes!("fixtures/header-repair-applied.bin");
/// See [`APPLIED_INPUT`].
const PREPARE_DEPARTS_INPUT: &[u8] = include_bytes!("fixtures/header-repair-prepare-departs.bin");
/// See [`APPLIED_INPUT`].
const APPLY_DEPARTS_INPUT: &[u8] = include_bytes!("fixtures/header-repair-apply-departs.bin");
/// See [`APPLIED_INPUT`].
const SUPPLIER_PRUNED_INPUT: &[u8] = include_bytes!("fixtures/header-repair-supplier-pruned.bin");

/// A committed input's file stem, its fixture, and how
/// [`regenerate_repair_seeds`] picks its Plan from [`support::byte_inputs`]:
/// the stream's count and longest input, then the index.
type Seed = (&'static str, RepairScenario, usize, usize, usize);

/// Every committed input, in the order of [`committed`]. The first three
/// repair, and [`SUPPLIER_PRUNED`] doesn't. Each fixture's history waits on
/// control steps with no deadline, such as the stale peer's close, so a
/// Plan that keeps one waiting until the step budget never reaches the
/// rest of it. The committed Plans and the generated ones reach every
/// fixture's history.
const SEEDS: [Seed; 4] = [
    ("header-repair-applied", RepairScenario::Applied, 4, 256, 0),
    (
        "header-repair-prepare-departs",
        RepairScenario::PrepareDeparts,
        4,
        256,
        0,
    ),
    (
        "header-repair-apply-departs",
        RepairScenario::ApplyDeparts,
        4,
        256,
        0,
    ),
    SUPPLIER_PRUNED,
];

/// The committed input whose supplier lacks the tree records.
const SUPPLIER_PRUNED: Seed = (
    "header-repair-supplier-pruned",
    RepairScenario::SupplierPruned,
    4,
    256,
    0,
);

/// The committed inputs, in the order of [`SEEDS`].
fn committed() -> [&'static [u8]; 4] {
    [
        APPLIED_INPUT,
        PREPARE_DEPARTS_INPUT,
        APPLY_DEPARTS_INPUT,
        SUPPLIER_PRUNED_INPUT,
    ]
}

/// The committed inputs whose repair applies, with their seeds.
fn repairing() -> impl Iterator<Item = (Seed, &'static [u8])> {
    SEEDS
        .into_iter()
        .zip(committed())
        .filter(|((_, scenario, ..), _)| *scenario != RepairScenario::SupplierPruned)
}

/// Polls `future` once. An engine port's operation outside a run finishes
/// on its first poll.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("an engine port call outside a run finishes on its first poll"),
    }
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// `scenario`'s scenario byte.
fn byte(scenario: RepairScenario) -> u8 {
    match scenario {
        RepairScenario::Applied => 0,
        RepairScenario::PrepareDeparts => 1,
        RepairScenario::ApplyDeparts => 2,
        RepairScenario::SupplierPruned => 3,
    }
}

/// The input [`regenerate_repair_seeds`] builds for `seed`: the fixture's
/// scenario byte, then its Plan.
fn generated((_, scenario, count, max_len, index): Seed) -> Vec<u8> {
    let mut input = vec![byte(scenario)];
    input.extend(support::byte_inputs(count, max_len).swap_remove(index));
    input
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[RepairHarness::SCENARIO_BYTES..])
}

/// One run of a fixture: its report, the node's stale calls, port and
/// outstanding-work peaks.
struct Run {
    report: RunReport,
    stale: StaleEvidence,
    node: std::sync::Arc<ControlledPort>,
    outstanding: OutstandingEvidence,
}

/// `scenario`'s run under `input`.
fn run(scenario: RepairScenario, input: PlanInput) -> Run {
    let ports = RepairHarness::prepare(&scenario);
    let stale = ports.stale_calls.clone();
    let node = ports.node.clone();
    let outstanding = ports.outstanding.clone();
    let report =
        run_prepared::<RepairHarness>(&scenario, ports, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    Run {
        report,
        stale,
        node,
        outstanding,
    }
}

/// The node's logged reactor facts, in order.
fn facts(report: &RunReport) -> Vec<&ReactorFact> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { event, .. } => Some(event),
            _ => None,
        })
        .collect()
}

/// The calls the node dispatched for a repair's prepare and apply, in
/// order, each as its call number and whether it's an apply.
fn repair_calls(facts: &[&ReactorFact]) -> Vec<(u64, bool)> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            ReactorFact::Dispatched {
                call,
                port: ReactorCall::Prepare { owner, .. },
            } if owner.kind == "body-repair" => Some((*call, false)),
            ReactorFact::Dispatched {
                call,
                port: ReactorCall::Apply { owner, .. },
            } if owner.kind == "body-repair" => Some((*call, true)),
            _ => None,
        })
        .collect()
}

/// How call `call` ended, if it did.
fn ended<'a>(facts: &[&'a ReactorFact], call: u64) -> Option<&'a str> {
    facts.iter().find_map(|fact| match fact {
        ReactorFact::CallEnded {
            call: ended, end, ..
        } if *ended == call => Some(end.as_str()),
        _ => None,
    })
}

/// Checks that every property of `run` held, didn't apply, or isn't
/// declared, as `sim/no-livelock` isn't here, that
/// `stale-completion-inert` applied, that its one stale call was the stale
/// peer's ordinary prepare, and that the repair prepared and applied once,
/// retired admitted, and left the node's records constraining the next
/// repair there.
///
/// A departure fixture holds the repair's prepare or apply until the node's
/// reactor has handled the supplier's session ending, so that call ending
/// at all shows it ended after the departure. Its call isn't stale, and the
/// round's own apply still follows.
fn repaired(run: &Run) {
    let report = &run.report;
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    for property in &report.properties {
        assert!(
            matches!(
                property.outcome,
                PropertyOutcome::Held { .. }
                    | PropertyOutcome::NotApplicable {
                        reason: NaReason::NeverApplied | NaReason::Undeclared
                    }
            ),
            "{property:?}"
        );
    }
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == STALE_COMPLETION_INERT)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "{outcome:?}"
    );

    let facts = facts(report);
    let calls = repair_calls(&facts);
    let [(prepare, false), (apply, true)] = calls[..] else {
        panic!("the repair prepares then applies once: {calls:?}");
    };
    assert_eq!(ended(&facts, prepare), Some("replied"));
    assert_eq!(ended(&facts, apply), Some("replied"));
    let retirements: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            ReactorFact::Retired {
                request, reason, ..
            } if request.owner.kind == "body-repair" => Some(reason.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(retirements, ["target_admitted"]);

    let windows = run.stale.windows();
    let [StaleWindow {
        call,
        apply: false,
        why: Staleness::RoundRetired,
        handled: true,
        ..
    }] = windows[..]
    else {
        panic!("only the stale peer's prepare ends stale: {windows:?}");
    };
    assert!(
        facts.iter().any(|fact| matches!(
            fact,
            ReactorFact::Dispatched {
                call: dispatched,
                port: ReactorCall::Prepare { owner, .. },
            } if *dispatched == call && owner.kind == "header"
        )),
        "the stale call is an ordinary prepare: {windows:?}"
    );

    // The supplier's rooted rows now constrain a repair at the height to
    // that one header.
    let engine = run.node.engine();
    let owner = BodyWorkAuthority::for_snapshot(&engine.snapshot())
        .bind(1, NonZeroU64::new(1).expect("one is nonzero"));
    let reply = ready(engine.vct_repair_context(owner, block::Height(REPAIR_HEIGHT)))
        .expect("the context reads");
    let VctRepairContextReply::Resolved(context) = reply else {
        panic!("the repair height still resolves: {reply:?}");
    };
    assert_eq!(context.selected_header_count(), 1);
}

/// `header-sync/stale-completion-inert` holds on each committed repairing
/// input through the registered harness, and each run repairs as
/// [`repaired`] checks.
#[test]
fn the_committed_repair_inputs_hold_stale_completion_inert() {
    let entry = harness::find(RepairHarness::NAME).expect("the harness is registered");
    for ((stem, scenario, ..), input) in repairing() {
        let result = (entry.witness)(
            &entry.scenario(&input[..RepairHarness::SCENARIO_BYTES]),
            plan(input),
            STALE_COMPLETION_INERT,
            Expect::Holds,
            &[],
            &mut MemorySink::default(),
        );
        assert_eq!(result, Ok(()), "{stem}");
        repaired(&run(scenario, plan(input)));
    }
}

/// Each repairing fixture repairs and holds under generated plans, not only
/// the committed ones, and the supplier-pruned one makes its charged
/// history under them as [`charged`] checks.
#[test]
fn the_repair_fixtures_hold_under_every_plan() {
    for input in support::inputs(8, 256) {
        for ((_, scenario, ..), _) in repairing() {
            repaired(&run(scenario, input.clone()));
        }
        charged(&run(RepairScenario::SupplierPruned, input));
    }
}

/// `header-sync/outstanding-work-bounded` holds on every committed input
/// through the registered harness.
#[test]
fn the_committed_repair_inputs_hold_outstanding_work_bounded() {
    let entry = harness::find(RepairHarness::NAME).expect("the harness is registered");
    for ((stem, ..), input) in SEEDS.into_iter().zip(committed()) {
        // Each run is a manifest history, which the acceptance tool
        // records from this declaration (C18).
        let result = run_declared(
            RepairHarness::NAME,
            &format!("committed {stem} input"),
            &entry.scenario(&input[..RepairHarness::SCENARIO_BYTES]),
            plan(input),
            |scenario, plan, sink| {
                (entry.witness)(
                    scenario,
                    plan,
                    OUTSTANDING_WORK_BOUNDED,
                    Expect::Holds,
                    &[],
                    sink,
                )
            },
        );
        assert_eq!(result, Ok(()), "committed {stem} input");
    }
    charged(&run(
        RepairScenario::SupplierPruned,
        plan(SUPPLIER_PRUNED_INPUT),
    ));
}

/// Checks that every property of the supplier-pruned `run` held or didn't
/// apply, that `outstanding-work-bounded` applied, that the repair retired
/// `history_pruned` and never prepared, and that the repair requirement
/// held the supplier's charge across its departure: one tried source and,
/// with the supplier gone, one departed one.
fn charged(run: &Run) {
    let report = &run.report;
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    for property in &report.properties {
        assert!(
            match &property.outcome {
                PropertyOutcome::Held { .. } => true,
                PropertyOutcome::NotApplicable { reason } => reason.is_benign(),
                PropertyOutcome::Violated { .. } => false,
            },
            "{property:?}"
        );
    }
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == OUTSTANDING_WORK_BOUNDED)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "{outcome:?}"
    );
    let facts = facts(report);
    assert_eq!(repair_calls(&facts), [], "the repair never prepares");
    let retirements: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            ReactorFact::Retired {
                request, reason, ..
            } if request.owner.kind == "body-repair" => Some(reason.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(retirements, ["history_pruned"]);
    let peaks = run.outstanding.peaks();
    assert_eq!(peaks.get("tried repair sources"), Some(&1), "{peaks:?}");
    assert_eq!(peaks.get("departed repair sources"), Some(&1), "{peaks:?}");
}

/// The scenario byte picks the fixture modulo 4.
#[test]
fn the_scenario_byte_picks_the_fixture() {
    for value in 0..=u8::MAX {
        let picked = RepairHarness::draw(&[value]);
        assert_eq!(byte(picked), value % 4, "{value}");
    }
}

/// The stale calls, the property's report and the full log depend only on
/// the plan: the committed prepare-departs plan A twice, then B, then A
/// again, gives A's every time, and B's log differs.
#[test]
fn repair_evidence_repeats_a_a_and_a_b_a() {
    let plans: [&[u8]; 2] = [
        &PREPARE_DEPARTS_INPUT[RepairHarness::SCENARIO_BYTES..],
        &[7; 256],
    ];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let run = run(RepairScenario::PrepareDeparts, Plan::from_fuzz(plan));
            let property = run
                .report
                .properties
                .iter()
                .find(|property| property.property == STALE_COMPLETION_INERT)
                .expect("the property reports");
            (
                format!("{:?}", run.report.log.entries()),
                format!("{:?}\n{property:?}", run.stale.windows()),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_repair_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_REPAIR_TRACE";

thread_local! {
    /// The stale calls of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<StaleEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/repair`'s own run, keeping its stale calls in [`CAPTURED`]
/// for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: RepairScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = RepairHarness::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.stale_calls.clone()));
    run_prepared::<RepairHarness>(&scenario, ports, input, sink, None)
}

/// The stale calls [`capture_run`] last kept, which must include one, as
/// text.
fn captured() -> String {
    let stale = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(stale.any(), "{:?}", stale.windows());
    format!("{:?}\n", stale.windows())
}

/// Each committed input, recorded in this process and replayed exactly in
/// a fresh process of the same test binary, finds the same stale calls at
/// the same drains.
#[test]
fn the_repair_witnesses_replay_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(RepairHarness::NAME).expect("the harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        // The parent reads the stale calls from the child's stdout.
        writeln!(std::io::stdout(), "STALE-BEGIN\n{}STALE-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    for input in committed() {
        let dir = TempDir::new().expect("a temporary directory");
        record(&entry, RecordInput::Input(input), dir.path()).expect("the witness records");
        let recorded = captured();
        let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "the_repair_witnesses_replay_in_a_fresh_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, dir.path())
            .output()
            .expect("the test binary runs");
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(child.status.success(), "{child:?}");
        let replayed = stdout
            .split_once("STALE-BEGIN\n")
            .and_then(|(_, rest)| rest.split_once("STALE-END"))
            .map(|(stale, _)| stale)
            .unwrap_or_else(|| panic!("the child prints its stale calls: {stdout}"));
        assert_eq!(replayed, recorded);
    }
}

/// Each committed input is its [`generated`] input, and runs through the
/// registered harness to a defined end that the explorer classifies clean.
#[test]
fn the_committed_inputs_are_their_plans_and_run_clean() {
    let entry = harness::find(RepairHarness::NAME).expect("the harness is registered");
    for (seed, committed) in SEEDS.into_iter().zip(committed()) {
        let stem = seed.0;
        assert_eq!(
            committed,
            generated(seed),
            "{stem}: the committed input changed; regenerate it if that's intended"
        );
        assert_eq!(RepairHarness::draw(committed), seed.1, "{stem}");
        let run = (entry.run_bytes)(committed, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{stem}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
    }
}

/// Rewrites the committed inputs and their sidecars. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_repair -- --ignored regenerate_repair_seeds`.
#[test]
#[ignore = "rewrites the committed inputs"]
fn regenerate_repair_seeds() {
    for seed in SEEDS {
        let (stem, scenario, ..) = seed;
        let input = generated(seed);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        // The repairing inputs witness the repair's own properties too. The
        // supplier-pruned input witnesses only the repair source rows.
        let witnesses: &[&str] = if scenario == RepairScenario::SupplierPruned {
            &[OUTSTANDING_WORK_BOUNDED]
        } else {
            &[
                STALE_COMPLETION_INERT,
                COMMIT_ONLY_AUTHORIZED,
                OUTSTANDING_WORK_BOUNDED,
            ]
        };
        let mut sidecar = format!(
            "# The committed fuzz input `{stem}.bin`: the harness that runs it, the\n\
             # generator version it was built for, and its SHA-256. Its first byte\n\
             # picks the repair fixture, then come the Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_repair.rs\"\n",
            harness = RepairHarness::NAME,
            generator = RepairHarness::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(scenario).expect("scenarios serialize")
            ),
        );
        for property in witnesses {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"holds\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
