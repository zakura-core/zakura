//! hdr-9b: `header-sync/stale-chain-effect-inert` on `header-sync/storage`.
//! Each witness holds and applies, with the hdr-6 controls holding or not
//! applying beside it. `cargo xtask planted
//! header-sync/stale-chain-effect-inert` checks that its bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, path::PathBuf, process::Command, rc::Rc};

use support::{byte_inputs, inputs};
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, STALE_CHAIN_EFFECT_INERT, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            adoption::AdoptionFact,
            policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
            Chain, ControlAction, ControlStep, Fork, Policy, Reset,
        },
        header_sync::{
            storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports,
            Storage, StorageScenario, StorageView,
        },
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest, StorageResult},
    property::{EndViolation, HasView, Property, Sources},
    MemorySink, Observation, ObservationLog, PlanInput, RunConfig, RunEnd, RunReport, Sim,
};

/// Where [`Viewed`] leaves its storage harness for the test to read.
type Slot = Rc<RefCell<Option<Rc<Storage>>>>;

/// `header-sync/storage`, also left in a slot, so a test can read its view
/// once the run ends.
struct Viewed(Rc<Storage>);

impl HasView for Viewed {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The storage harness's stamped facts.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Viewed {
    const NAME: &'static str = "test/viewed-storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = (Ports, Slot);

    /// The storage harness's config.
    fn config(_: &StorageScenario) -> RunConfig {
        storage_config()
    }

    /// The storage harness's ports.
    fn prepare(scenario: &StorageScenario) -> (Ports, Slot) {
        (Storage::prepare(scenario), Slot::default())
    }

    /// The storage harness, also left in the slot.
    fn build(sim: &mut Sim, scenario: &StorageScenario, (ports, slot): (Ports, Slot)) -> Self {
        let storage = Rc::new(Storage::build(sim, scenario, ports));
        *slot.borrow_mut() = Some(storage.clone());
        Viewed(storage)
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: the scenarios are built by hand.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("the scenarios are built by hand")
    }
}

/// A control step on the node under test.
fn node_step(action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: "node".to_owned(),
        action,
        when,
    }
}

/// A mirror holding `chain`, which dials the node.
fn mirror(chain: Chain) -> MirrorSpec {
    MirrorSpec {
        chain,
        dialer: Dialer::Mirror,
    }
}

/// The node at A2 syncing from `mirror-0` at A6. Its first apply commits
/// with its reply held until an invalidation of the node's tip, made once
/// the apply commits. The commit stays, the reactor adopts the reset and
/// retires the round obsolete, and the late reply is inert.
pub fn pre_reset_commit() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![mirror(Chain::main(6))],
        policy: Policy {
            reply_holds: vec![ReplyHold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Reset("node".to_owned()),
            }],
            ..Policy::converging()
        },
        controls: vec![node_step(
            ControlAction::Reset(Reset::Invalidate { depth: 0 }),
            Some(Trigger::Committed(Select::nth(OpKind::Apply, 1))),
        )],
        weights: Default::default(),
    }
}

/// [`pre_reset_commit`] with the first apply itself held until the reset,
/// then a reconsider once it completes: the apply runs after the reset,
/// and the engine refuses it as stale.
pub fn refused_across_reset() -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Reset("node".to_owned()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![
            node_step(
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::Submitted(Select::nth(OpKind::Apply, 1))),
            ),
            node_step(
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
            ),
        ],
        ..pre_reset_commit()
    }
}

/// The node at A2 with `mirror-0` at A6 and `mirror-1` on a fork of A2,
/// four headers long. The first apply is held until the second completes,
/// so one fork's commit bumps the generation while the other fork's apply
/// waits under the old one, on a branch that stays eligible. The held
/// apply then runs, and the engine refuses it as stale.
pub fn fork_stale_apply() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            mirror(Chain::main(6)),
            mirror(Chain {
                main: 2,
                fork: Some(Fork { branch: 1, len: 4 }),
            }),
        ],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Completed(Select::nth(OpKind::Apply, 2)),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: Vec::new(),
        weights: Default::default(),
    }
}

/// The node and `mirror-0` both at A3, so each sends the other one
/// `Status` at the link. An invalidation of the node's tip lands 200 ms in,
/// inside the 1 s publication floor of the status the node just queued.
/// The reactor adopts the reset at once, and queues the next `Status` only
/// once the floor passes. A reconsider follows.
pub fn reset_inside_status_floor() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(3))],
        policy: Policy::converging(),
        controls: vec![
            node_step(
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::VirtualTime(200)),
            ),
            node_step(
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::VirtualTime(3_000)),
            ),
        ],
        weights: Default::default(),
    }
}

/// Every witness, by name.
fn witnesses() -> [(&'static str, StorageScenario); 4] {
    [
        ("pre-reset-commit", pre_reset_commit()),
        ("refused-across-reset", refused_across_reset()),
        ("fork-stale-apply", fork_stale_apply()),
        ("reset-inside-status-floor", reset_inside_status_floor()),
    ]
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure, and returns the report and the view after it.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, StorageView) {
    let prepared = Viewed::prepare(scenario);
    let slot = prepared.1.clone();
    let report =
        run_prepared::<Viewed>(scenario, prepared, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let storage = slot.borrow_mut().take().expect("the harness was built");
    (report, storage.view())
}

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// The node's storage facts, with their steps, in log order.
fn storage(report: &RunReport) -> Vec<(u64, StorageFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0, fact, ..
            } => Some((entry.step, fact.clone())),
            _ => None,
        })
        .collect()
}

/// The IDs of the node's apply operations, in submission order.
fn applies(report: &RunReport) -> Vec<u64> {
    storage(report)
        .into_iter()
        .filter_map(|(_, fact)| match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ApplyHeaderTarget { .. },
                ..
            } => Some(id),
            _ => None,
        })
        .collect()
}

/// Operation `id`'s `Completed` result and step.
fn completed(report: &RunReport, id: u64) -> Option<(u64, StorageResult)> {
    storage(report)
        .into_iter()
        .find_map(|(step, fact)| match fact {
            StorageFact::Completed {
                id: done, result, ..
            } if done == id => Some((step, result)),
            _ => None,
        })
}

/// The stale refusal an apply gets when its generation is no longer the
/// engine's.
fn refused() -> StorageResult {
    StorageResult::ApplyFailed {
        category: "stale_target_or_generation".to_owned(),
    }
}

/// Each witness holds `header-sync/stale-chain-effect-inert` and applies,
/// and the hdr-6 controls hold or don't apply beside it.
#[test]
fn every_witness_holds_and_applies() {
    for (name, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let (report, _) = run(&scenario, input);
            assert!(
                matches!(
                    outcome(&report, STALE_CHAIN_EFFECT_INERT),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{name}: {:?}",
                outcome(&report, STALE_CHAIN_EFFECT_INERT)
            );
            for control in [
                CHUNK_BUDGET_BOUND,
                TEARDOWN_RETURNS_BASELINE,
                STALE_COMPLETION_INERT,
            ] {
                assert!(
                    matches!(
                        outcome(&report, control),
                        PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable
                    ),
                    "{name}: {control}: {:?}",
                    outcome(&report, control)
                );
            }
        }
    }
}

/// Each witness takes the path it names. The pre-reset commit's apply
/// commits before the reset, its reply reaches the call after the reset,
/// and its round retires obsolete before the reactor handles that reply. The apply held across a reset, and the one held
/// while the other fork commits, are each refused stale after the
/// generation moved. In the status-floor witness, the reactor adopts the
/// reset's generation before its next `Status`, which comes a second or
/// more after the one before, with no deferral.
#[test]
fn each_witness_takes_the_path_it_names() {
    for input in inputs(8, 256) {
        let (report, _) = run(&pre_reset_commit(), input.clone());
        let apply = applies(&report)[0];
        let facts = storage(&report);
        let committed = facts
            .iter()
            .find(|(_, fact)| matches!(fact, StorageFact::Committed { id, .. } if *id == apply))
            .map(|(step, _)| *step)
            .expect("the apply commits");
        let reset = facts
            .iter()
            .find(|(_, fact)| matches!(fact, StorageFact::Reset { .. }))
            .map(|(step, _)| *step)
            .expect("the node resets");
        let (delivered, _) = completed(&report, apply).expect("the reply arrives");
        assert!(committed <= reset && reset <= delivered);
        let call = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match &entry.obs {
                Observation::Reactor {
                    event: ReactorFact::CallEnded { call, op, .. },
                    ..
                } if *op == Some(apply) => Some(*call),
                _ => None,
            })
            .expect("the apply's call ends");
        let step_of = |wanted: &dyn Fn(&ReactorFact) -> bool| {
            report
                .log
                .entries()
                .iter()
                .find_map(|entry| match &entry.obs {
                    Observation::Reactor { event, .. } if wanted(event) => Some(entry.step),
                    _ => None,
                })
        };
        let retired = step_of(&|event| {
            matches!(event, ReactorFact::Retired { reason, .. } if reason == "snapshot_obsolete")
        })
        .expect("the round retires obsolete");
        let handled = step_of(&|event| matches!(event, ReactorFact::Handled { call: handled, .. } if *handled == call))
            .expect("the late reply is handled");
        assert!(reset <= retired && retired < handled, "{retired} {handled}");

        let (report, _) = run(&refused_across_reset(), input.clone());
        let apply = applies(&report)[0];
        assert_eq!(
            completed(&report, apply).map(|(_, result)| result),
            Some(refused())
        );

        let (report, _) = run(&fork_stale_apply(), input.clone());
        let [first, second, ..] = applies(&report)[..] else {
            panic!("both forks apply");
        };
        let (other, _) = completed(&report, second).expect("the other fork completes");
        let (held, result) = completed(&report, first).expect("the held apply completes");
        assert!(other < held);
        assert_eq!(result, refused());

        let (_, view) = run(&reset_inside_status_floor(), input);
        let facts = view.adoption.facts();
        let reset_at = facts
            .iter()
            .filter(|stamped| matches!(stamped.fact, AdoptionFact::Adopted(_)))
            .nth(1)
            .map(|stamped| stamped.at)
            .expect("the reactor adopts the reset");
        let statuses: Vec<_> = facts
            .iter()
            .filter(|stamped| matches!(stamped.fact, AdoptionFact::StatusQueued { .. }))
            .map(|stamped| stamped.at)
            .collect();
        let before = statuses.iter().rev().find(|at| **at <= reset_at);
        let after = statuses.iter().find(|at| **at > reset_at);
        let (Some(before), Some(after)) = (before, after) else {
            panic!("a status before and after the reset's adoption: {statuses:?}");
        };
        assert!(
            *after >= *before + std::time::Duration::from_secs(1),
            "{before:?} {after:?}"
        );
        assert!(!facts
            .iter()
            .any(|stamped| matches!(stamped.fact, AdoptionFact::StatusDeferred { .. })));
    }
}

/// The witnesses `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-chain-<name>.json` with its Plan bytes beside
/// it in `header-chain-<name>.plan`.
fn committed_witnesses() -> [(&'static str, StorageScenario); 2] {
    [
        ("fork-stale-apply", fork_stale_apply()),
        ("pre-reset-commit", pre_reset_commit()),
    ]
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-chain-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-chain-{name}.{extension}"))
}

/// Each committed witness is its builder's scenario, as JSON, with the
/// fixed Plan bytes, and holds and applies under them.
#[test]
fn the_committed_witnesses_match_their_builders() {
    for (name, scenario) in committed_witnesses() {
        let committed: StorageScenario = serde_json::from_str(
            &fs::read_to_string(fixture(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            serde_json::to_value(&committed).expect("scenarios serialize"),
            serde_json::to_value(&scenario).expect("scenarios serialize"),
            "{name}: run `regenerate_chain_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_chain_witnesses`"
        );
        let (report, _) = run(&scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert!(
            matches!(
                outcome(&report, STALE_CHAIN_EFFECT_INERT),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed witnesses from their builders.
#[test]
#[ignore = "rewrites the committed stale-chain witnesses"]
fn regenerate_chain_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// The pre-reset-commit witness's log and the property's report depend only
/// on the scenario and the plan: plan A twice, then B, then A again, gives
/// A's every time, and B's log differs.
#[test]
fn the_chain_witness_repeats_a_a_and_a_b_a() {
    let scenario = pre_reset_commit();
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, _) = run(&scenario, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}", outcome(&report, STALE_CHAIN_EFFECT_INERT)),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// `record --witness` judges each committed witness as `cargo xtask
/// planted` runs it, expecting a hold, and `replay` reruns the trace Exact
/// in a fresh process.
#[test]
fn the_committed_witnesses_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_witnesses() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario = fs::read_to_string(fixture(name, "json")).expect("the witness is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture(name, "plan").to_str().expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            STALE_CHAIN_EFFECT_INERT,
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
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
