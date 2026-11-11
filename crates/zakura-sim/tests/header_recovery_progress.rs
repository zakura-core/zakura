//! hdr-11: `header-sync/bounded-adversary-progress` on
//! `header-sync/liveness`'s recovery fixtures. Each witness holds the
//! property, with every other property holding or not applying beside it.
//! `cargo xtask planted header-sync/bounded-adversary-progress` checks that
//! its bypass breaks it.
//!
//! The recovery policy picks the action enabled the longest, and the
//! harness offers `recovered` only at a quiet step, so with no hostile
//! prefix the node has synced from its source by the `recovered` pick. Each
//! witness here is met at the pick. A unit test in `recovery.rs` covers a
//! later adoption: the property's obligation discharges only on a header
//! the objective holds. The bypass runs to the deadline.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, path::PathBuf, process::Command, rc::Rc};

use support::{byte_inputs, inputs};
use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::{after_step, PropertyOutcome},
    domain::header_sync_progress::{BOUNDED_ADVERSARY_PROGRESS, RECOVERY_PROGRESS_BOUND},
    harness::{
        header_node::{
            recovery::{RecoveryEvidence, RecoveryObjective},
            Chain, Fork,
        },
        header_sync::{
            liveness::{LivenessScenario, RecoveryFixture, RECOVERY_SOURCE},
            Liveness, Weights,
        },
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, Outcome, PlanInput, RunReport,
};

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// One step's pick and time.
#[derive(Clone, Debug)]
struct Pick {
    step: u64,
    identity: String,
    now: SimInstant,
}

/// A recovery fixture's run and what it left.
struct Run {
    report: RunReport,
    picks: Vec<Pick>,
    recovery: RecoveryEvidence,
}

impl Run {
    /// The first step that picked `identity`.
    fn picked(&self, identity: &str) -> &Pick {
        self.picks
            .iter()
            .find(|pick| pick.identity == identity)
            .unwrap_or_else(|| panic!("{identity} is picked"))
    }

    /// The steps that completed an apply, in order.
    fn applies_completed(&self) -> Vec<u64> {
        let mut applies = Vec::new();
        let mut committed = Vec::new();
        for entry in self.report.log.entries() {
            match &entry.obs {
                Observation::Storage {
                    fact:
                        StorageFact::Submitted {
                            id,
                            request: StorageRequest::ApplyHeaderTarget { .. },
                            ..
                        },
                    ..
                } => applies.push(*id),
                Observation::Storage {
                    fact: StorageFact::Completed { id, .. },
                    ..
                } if applies.contains(id) => committed.push(entry.step),
                _ => {}
            }
        }
        committed
    }
}

/// Runs recovery `fixture` under `input`, which must end idle, after its
/// finish, never at its budget, with no violation or harness failure, and
/// with every property but this one holding or not applying.
fn run(fixture: RecoveryFixture, input: PlanInput) -> Run {
    let scenario = LivenessScenario::Recovery(fixture);
    let ports = Liveness::prepare(&scenario);
    let recovery = ports.recovery.clone();
    let picks = Rc::new(RefCell::new(Vec::new()));
    let into = picks.clone();
    let mut checker = after_step(move |cx| {
        into.borrow_mut().push(Pick {
            step: cx.step,
            identity: cx.pick.to_string(),
            now: cx.now,
        });
        None
    });
    let report = run_prepared::<Liveness>(
        &scenario,
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut checker),
    )
    .unwrap_or_else(|error| panic!("{fixture:?}: {error}"));
    let end = &report.end;
    assert!(
        matches!(end.outcome, Outcome::Idle { .. }),
        "{fixture:?}: {end:?}"
    );
    assert_eq!(
        (&end.violation, &end.harness),
        (&None, &None),
        "{fixture:?}: {end:?}"
    );
    for property in &report.properties {
        if property.property != BOUNDED_ADVERSARY_PROGRESS {
            assert!(
                matches!(
                    property.outcome,
                    PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable
                ),
                "{fixture:?}: {}: {:?}",
                property.property,
                property.outcome
            );
        }
    }
    let picks = picks.take();
    Run {
        report,
        picks,
        recovery,
    }
}

/// The recovery control: under 8 plans the node, at A3 at the `Recover`
/// pick, owes A4, its only member's tip. Storage completes A4's apply after
/// the `Recover` pick and before the `recovered` pick, so a baseline taken
/// at `recovered` would owe nothing, and the one taken at `Recover` keeps
/// the run applicable: the plan's early-successor control. The node has
/// adopted A4 by the `recovered` pick, which discharges it there, well
/// inside the 30-second bound.
#[test]
#[allow(clippy::print_stdout)]
fn the_recovery_control_holds_at_its_recovered_pick() {
    for input in inputs(8, 256) {
        let run = run(RecoveryFixture::Successor, input);
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        let recover = run.picked(&format!("peer:{RECOVERY_SOURCE}:recover"));
        let recovered = run.picked("peer:node:recovered");
        let (record, objective) = run.recovery.objective().expect("the pick wrote its record");
        let baseline = run
            .recovery
            .baseline()
            .expect("the recover kept a baseline");
        assert_eq!(baseline.chain.last().map(|tip| tip.height.0), Some(3));
        let RecoveryObjective { headers, .. } = objective.expect("A4 is owed");
        let [successor] = headers[..] else {
            panic!("one successor: {headers:?}");
        };
        assert_eq!(successor.height.0, 4);
        assert_eq!(record.node_tip.0, successor, "met at the pick");
        let [committed] = run.applies_completed()[..] else {
            panic!("one apply completes");
        };
        assert!(recover.step < committed && committed < recovered.step);
        let waited = recovered.now.saturating_duration_since(recover.now);
        assert!(waited < RECOVERY_PROGRESS_BOUND, "{waited:?}");
        println!("recover at {:?}, met at {:?}", recover.now, recovered.now);
    }
}

/// The property's report and the full log depend only on the fixture and
/// the plan: the recovery control under plan A twice, then B, then A again,
/// gives A's every time, and B's log differs.
#[test]
fn the_recovery_control_repeats_a_a_and_a_b_a() {
    let plans = [committed_plan(), byte_inputs(16, 256).remove(6)];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let run = run(
                RecoveryFixture::Successor,
                zakura_sim::Plan::from_fuzz(plan),
            );
            (
                format!("{:?}", run.report.log.entries()),
                format!("{:?}", outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS)),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// The fixtures `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-recovery-<name>.json` with its Plan bytes
/// beside it in `header-recovery-<name>.plan`.
fn committed_fixtures() -> [(&'static str, LivenessScenario); 4] {
    [
        ("successor", RecoveryFixture::Successor),
        ("closes", RecoveryFixture::Closes),
        ("faults", RecoveryFixture::Faults),
        ("departure", RecoveryFixture::Departure),
    ]
    .map(|(name, fixture)| (name, LivenessScenario::Recovery(fixture)))
}

/// The Plan bytes every committed fixture runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture file `header-recovery-<name>.<extension>`.
fn fixture_file(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-recovery-{name}.{extension}"))
}

/// Each committed fixture is its named fixture, as JSON, with the fixed
/// Plan bytes, and holds and applies under them.
#[test]
fn the_committed_fixtures_match_their_names() {
    for (name, fixture) in committed_fixtures() {
        let committed: LivenessScenario = serde_json::from_str(
            &fs::read_to_string(fixture_file(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            committed, fixture,
            "{name}: run `regenerate_recovery_fixtures`"
        );
        let plan =
            fs::read(fixture_file(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_recovery_fixtures`"
        );
        let LivenessScenario::Recovery(recovery) = fixture else {
            unreachable!("committed fixtures are recovery fixtures");
        };
        let run = run(recovery, zakura_sim::Plan::from_fuzz(&plan));
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 },
            "{name}"
        );
    }
}

/// Rewrites the committed fixtures from their names.
#[test]
#[ignore = "rewrites the committed recovery fixtures"]
fn regenerate_recovery_fixtures() {
    for (name, fixture) in committed_fixtures() {
        let json = serde_json::to_string_pretty(&fixture).expect("fixtures serialize");
        fs::write(fixture_file(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture_file(name, "plan"), committed_plan())
            .expect("the fixture directory exists");
    }
}

/// `record --witness` judges each committed fixture as `cargo xtask
/// planted` runs it, expecting a hold, the trace ends idle, and `replay`
/// reruns it Exact in a fresh process.
#[test]
fn the_committed_fixtures_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_fixtures() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario =
            fs::read_to_string(fixture_file(name, "json")).expect("the fixture is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Liveness::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture_file(name, "plan")
                .to_str()
                .expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            BOUNDED_ADVERSARY_PROGRESS,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{name}: {witness}"
        );
        let end: serde_json::Value = fs::read_to_string(trace.join("trace.jsonl"))
            .expect("the trace is written")
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|line| line.get("end").cloned())
            .expect("the trace ends");
        assert_eq!(end["outcome"], "idle", "{name}: {end}");
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// The fixture headers of `chain` above `height`, in height order.
fn above(chain: Chain, height: u32) -> Vec<Frontier> {
    let fixture = Weights::default().fixture();
    let mut headers = Vec::new();
    for (parent, segment) in chain.segments(&fixture) {
        for (offset, header) in (1..).zip(segment) {
            let at = parent.height.0 + offset;
            headers.push(Frontier::new(block::Height(at), header.hash()));
        }
    }
    headers.retain(|header| header.height.0 > height);
    headers
}

/// `main` headers on the main branch, then `len` on branch 1.
fn fork(main: u32, len: u32) -> Chain {
    Chain {
        main,
        fork: Some(Fork { branch: 1, len }),
    }
}

/// Every other recovery control holds under 4 plans, each but `Faults` met
/// at the `recovered` pick, with the objective its doc names. Mirrors sync from
/// the node too, so a member's recorded chain can be one it took from the
/// node, and the expected headers come from the fixture's chains:
///
/// - `Fork`: A4 and A5. `mirror-1`'s B4, heavier than the baseline,
///   doesn't meet it.
/// - `SharedPrefix`: A4 to A6, so `mirror-1`'s A4 meets it, and its B5
///   doesn't.
/// - `Invalidation`: from a baseline at A2, A3 and A4, so the header the
///   `Recover` reconsidered counts.
/// - `EqualWork`: both tied tips, A4 and B4.
/// - `Reconsideration`: from a baseline at A2, the node's own branch B3 to
///   B5, which it holds at the pick. The source's lighter A3 and A4 don't
///   meet it.
///
/// - `Closes`, `Faults` and `Departure` (hdr-13's prefix classes): the
///   source's A6 chain past the baseline, which is A3 after `Faults`'
///   failed prepares.
///
/// `SyncedFirst`'s baseline already holds A4, so nothing is owed and the
/// property doesn't apply.
#[test]
fn every_recovery_control_holds() {
    for fixture in RecoveryFixture::all() {
        for input in inputs(4, 256) {
            let run = run(fixture, input);
            let (record, objective) = run.recovery.objective().expect("the pick wrote its record");
            let baseline = run
                .recovery
                .baseline()
                .expect("the recover kept a baseline");
            let base = baseline
                .chain
                .last()
                .expect("a chain has its anchor")
                .height
                .0;
            if fixture == RecoveryFixture::SyncedFirst {
                assert_eq!(base, 4, "{fixture:?}");
                assert_eq!(objective, None, "{fixture:?}");
                assert_eq!(
                    outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
                    &PropertyOutcome::NotApplicable,
                    "{fixture:?}"
                );
                continue;
            }
            assert_eq!(
                outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
                &PropertyOutcome::Held { applicable: 1 },
                "{fixture:?}"
            );
            let objective = objective.expect("a successor is owed");
            // A failed prepare leaves the node waiting out a retry, so after
            // `Faults` it can meet the objective after the pick.
            assert!(
                fixture == RecoveryFixture::Faults || objective.met_by(&record.node_tip.0),
                "{fixture:?}: met at the pick"
            );
            let (mut expected, unmet) = match fixture {
                RecoveryFixture::Successor => (above(Chain::main(4), 3), Vec::new()),
                RecoveryFixture::Fork => (above(Chain::main(5), 3), above(fork(3, 1), 3)),
                RecoveryFixture::SharedPrefix => {
                    assert!(objective.met_by(&above(fork(4, 1), 3)[0]), "{fixture:?}");
                    (above(Chain::main(6), 3), above(fork(4, 1), 4))
                }
                RecoveryFixture::Invalidation => {
                    assert_eq!(base, 2, "{fixture:?}");
                    (above(Chain::main(4), 2), Vec::new())
                }
                RecoveryFixture::EqualWork => {
                    let mut tied = above(Chain::main(4), 3);
                    tied.extend(above(fork(3, 1), 3));
                    (tied, Vec::new())
                }
                RecoveryFixture::Reconsideration => {
                    assert_eq!(base, 2, "{fixture:?}");
                    assert_eq!(record.node_tip.0.height.0, 5, "{fixture:?}");
                    (above(fork(2, 3), 2), above(Chain::main(4), 2))
                }
                RecoveryFixture::Faults => {
                    assert_eq!(base, 3, "{fixture:?}: every prepare failed");
                    (above(Chain::main(6), 3), Vec::new())
                }
                RecoveryFixture::Closes | RecoveryFixture::Departure => {
                    (above(Chain::main(6), base), Vec::new())
                }
                RecoveryFixture::SyncedFirst => unreachable!("judged above"),
            };
            let mut headers = objective.headers.clone();
            headers.sort_by_key(|header| (header.height, header.hash.0));
            expected.sort_by_key(|header| (header.height, header.hash.0));
            assert_eq!(headers, expected, "{fixture:?}");
            for header in unmet {
                assert!(!objective.met_by(&header), "{fixture:?}: {header:?}");
            }
        }
    }
}
