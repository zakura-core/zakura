//! hdr-12a: `header-sync/expected-chain-selected` on `header-sync/storage`.
//! Each witness holds and applies, with the hdr-6 controls holding or not
//! applying beside it. `cargo xtask planted
//! header-sync/expected-chain-selected` checks that its bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::HashSet, fs, path::PathBuf, process::Command};

use support::{byte_inputs, inputs};
use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::Frontier;
use zakura_sim::{
    check::{after_step, PropertyOutcome},
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, EXPECTED_CHAIN_SELECTED, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            policy::{OpKind, Select, Trigger},
            Base, Chain, ControlAction, ControlStep, Fork, Policy, Reset,
        },
        header_sync::{self, Dialer, MirrorSpec, Storage, StorageScenario, Weights},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest, StorageResult},
    MemorySink, Observation, PlanInput, RunReport,
};
use zakura_storage::header::fixture::HeaderFixture;

/// The fork fixture's storage: every operation succeeds within 10 ms, far
/// below the 30 s request timeout, with no holds.
fn fork_policy() -> Policy {
    Policy {
        base: Base::SuccessOnly,
        bound_ms: Some(10),
        holds: Vec::new(),
        reply_holds: Vec::new(),
    }
}

/// A mirror holding `len` headers on `branch`, forked from genesis, which
/// `dialer` links.
fn fork(branch: u8, len: u32, dialer: Dialer) -> MirrorSpec {
    MirrorSpec {
        chain: Chain {
            main: 0,
            fork: Some(Fork { branch, len }),
        },
        dialer,
        max_headers_per_response: None,
    }
}

/// The fork fixture: the node at genesis and `mirrors` on fixed forks from
/// it, under `weights`, with no controls.
fn fork_fixture(mirrors: Vec<MirrorSpec>, weights: Weights) -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors,
        weights,
        policy: fork_policy(),
        controls: Vec::new(),
    }
}

/// Branch 2 at weight 2, so its three headers outweigh branch 1's five.
fn heavier_short() -> Weights {
    Weights {
        branch_1: 0,
        branch_2: 2,
    }
}

/// The stronger fork offered first: `mirror-0` dials the node with three
/// headers of weighted branch 2, and the node dials `mirror-1`, which holds
/// five unweighted headers of branch 1. The shorter fork has more work.
pub fn stronger_first() -> StorageScenario {
    fork_fixture(
        vec![fork(2, 3, Dialer::Mirror), fork(1, 5, Dialer::Node)],
        heavier_short(),
    )
}

/// [`stronger_first`] with the mirrors swapped, so the stronger fork is
/// offered last, from the other mirror.
pub fn stronger_last() -> StorageScenario {
    fork_fixture(
        vec![fork(1, 5, Dialer::Mirror), fork(2, 3, Dialer::Node)],
        heavier_short(),
    )
}

/// Three mirrors from genesis: four headers on the main line, five on
/// branch 1 at weight 1 and three on branch 2 at weight 2. Branch 2 has
/// the most work and is the shortest.
pub fn three_forks() -> StorageScenario {
    StorageScenario {
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(4),
                dialer: Dialer::Mirror,
                max_headers_per_response: None,
            },
            fork(1, 5, Dialer::Node),
            fork(2, 3, Dialer::Mirror),
        ],
        ..fork_fixture(
            Vec::new(),
            Weights {
                branch_1: 1,
                branch_2: 2,
            },
        )
    }
}

/// Equal work: four headers on each of branches 1 and 2, both at weight
/// 1. Either tip holds.
pub fn equal_work() -> StorageScenario {
    fork_fixture(
        vec![fork(1, 4, Dialer::Mirror), fork(2, 4, Dialer::Node)],
        Weights {
            branch_1: 1,
            branch_2: 1,
        },
    )
}

/// Outside the fork fixture: the node holds branch 1's five unweighted
/// headers, and `mirror-0` offers branch 2's three weighted ones, which
/// have more work. The node fetches the shorter fork for its work, and the
/// engine, holding both, selects it.
pub fn heavier_offered_to_longer() -> StorageScenario {
    StorageScenario {
        node: Chain {
            main: 0,
            fork: Some(Fork { branch: 1, len: 5 }),
        },
        mirrors: vec![fork(2, 3, Dialer::Mirror)],
        weights: heavier_short(),
        policy: Policy::converging(),
        controls: Vec::new(),
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

/// Outside the fork fixture: the node holds weighted branch 2's three
/// headers, and `mirror-0` five lighter ones of branch 1. An invalidation of
/// branch 2's first header makes the lighter fork the heaviest eligible, so
/// the node fetches and selects it. Once that apply completes, a
/// reconsider makes branch 2 eligible again, and the engine selects it.
pub fn invalidated_then_reconsidered() -> StorageScenario {
    StorageScenario {
        node: Chain {
            main: 0,
            fork: Some(Fork { branch: 2, len: 3 }),
        },
        mirrors: vec![fork(1, 5, Dialer::Mirror)],
        weights: heavier_short(),
        policy: Policy::converging(),
        controls: vec![
            node_step(ControlAction::Reset(Reset::Invalidate { depth: 2 }), None),
            node_step(
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
            ),
        ],
    }
}

/// The node and `mirror-0` both at genesis, and a harness commit of two
/// main-line headers straight into the node's engine, outside the
/// controlled port's runs. The node has nothing to fetch, so the commit's
/// record is the only one past the seed, whose score is zero work.
pub fn harness_commit() -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![MirrorSpec {
            chain: Chain::default(),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        }],
        policy: Policy::converging(),
        controls: vec![node_step(ControlAction::Commit { len: 2 }, None)],
        weights: Weights::default(),
    }
}

/// Every witness, by name.
fn witnesses() -> [(&'static str, StorageScenario); 7] {
    [
        ("stronger-first", stronger_first()),
        ("stronger-last", stronger_last()),
        ("three-forks", three_forks()),
        ("equal-work", equal_work()),
        ("heavier-offered-to-longer", heavier_offered_to_longer()),
        (
            "invalidated-then-reconsidered",
            invalidated_then_reconsidered(),
        ),
        ("harness-commit", harness_commit()),
    ]
}

/// A chain's tip and its work above genesis, from the fixture's targets.
fn tip_and_work(fixture: &HeaderFixture, chain: &Chain) -> (Frontier, U256) {
    let work = chain
        .segments(fixture)
        .iter()
        .flat_map(|(_, headers)| headers.iter())
        .map(|header| {
            U256::from(
                header
                    .difficulty_threshold
                    .to_work()
                    .expect("fixture targets are valid"),
            )
        })
        .fold(U256::zero(), |sum, work| sum + work);
    (chain.tip(fixture), work)
}

/// The tips of `scenario`'s mirror chains with the most work.
fn heaviest_mirror_tips(scenario: &StorageScenario) -> HashSet<block::Hash> {
    let fixture = scenario.weights.fixture();
    let tips: Vec<(Frontier, U256)> = scenario
        .mirrors
        .iter()
        .map(|mirror| tip_and_work(&fixture, &mirror.chain))
        .collect();
    let best = tips.iter().map(|(_, work)| *work).max().expect("mirrors");
    tips.into_iter()
        .filter(|(_, work)| *work == best)
        .map(|(tip, _)| tip.hash)
        .collect()
}

/// The target of the node's first apply to run in its engine, as hex: the
/// first that committed with its reply held, or completed `Applied`.
fn first_executed_apply_target(report: &RunReport) -> Option<String> {
    let mut targets = std::collections::HashMap::new();
    report.log.entries().iter().find_map(|entry| {
        let Observation::Storage {
            instance: 0, fact, ..
        } = &entry.obs
        else {
            return None;
        };
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ApplyHeaderTarget { target, .. },
                ..
            } => {
                targets.insert(*id, target.clone());
                None
            }
            StorageFact::Committed { id, .. }
            | StorageFact::Completed {
                id,
                result: StorageResult::Applied,
                ..
            } => targets.get(id).cloned(),
            _ => None,
        }
    })
}

/// What a run of a witness left: its report, every tip the node's engine
/// selected after a step, in order, whether its engine held each mirror's
/// tip at the end, and the settled points the property judged: steps after
/// which no task could run and whose drain found every other settled-point
/// condition.
struct Ran {
    report: RunReport,
    selected: Vec<block::Hash>,
    holds_every_mirror_tip: bool,
    settled_points: u64,
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Ran {
    let ports = Storage::prepare(scenario);
    let port = ports.node.clone();
    let evidence = ports.fork_choice.clone();
    let selected = RefCell::new(Vec::<block::Hash>::new());
    let settled_points = std::cell::Cell::new(0);
    let mut watch = after_step(|cx| {
        let tip = port.engine().snapshot().frontiers.header_best.hash;
        let mut seen = selected.borrow_mut();
        if seen.last() != Some(&tip) {
            seen.push(tip);
        }
        let at_rest = matches!(
            cx.status,
            zakura_scheduler::Status::Quiescent
                | zakura_scheduler::Status::WaitingOnTime
                | zakura_scheduler::Status::WaitingOnExternal
        );
        if at_rest && evidence.settled_at(cx.step).is_some() {
            settled_points.set(settled_points.get() + 1);
        }
        None
    });
    let report = run_prepared::<Storage>(
        scenario,
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut watch),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    drop(watch);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let fixture = scenario.weights.fixture();
    let held = port.engine().durable_state().graph.nodes;
    let holds_every_mirror_tip = scenario
        .mirrors
        .iter()
        .all(|mirror| held.contains_key(&mirror.chain.tip(&fixture).hash));
    Ran {
        report,
        selected: selected.into_inner(),
        holds_every_mirror_tip,
        settled_points: settled_points.get(),
    }
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

/// Each witness holds `header-sync/expected-chain-selected` and applies,
/// and the hdr-6 controls hold or don't apply beside it.
#[test]
fn every_witness_holds_and_applies() {
    for (name, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let ran = run(&scenario, input);
            assert!(
                matches!(
                    outcome(&ran.report, EXPECTED_CHAIN_SELECTED),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{name}: {:?}",
                outcome(&ran.report, EXPECTED_CHAIN_SELECTED)
            );
            for control in [
                CHUNK_BUDGET_BOUND,
                TEARDOWN_RETURNS_BASELINE,
                STALE_COMPLETION_INERT,
            ] {
                assert!(
                    match outcome(&ran.report, control) {
                        PropertyOutcome::Held { .. } => true,
                        PropertyOutcome::NotApplicable { reason } => reason.is_benign(),
                        PropertyOutcome::Violated { .. } => false,
                    },
                    "{name}: {control}: {:?}",
                    outcome(&ran.report, control)
                );
            }
        }
    }
}

/// Each fork witness offers a real choice: the node ends holding every
/// mirror's tip and selecting one of the heaviest, whichever mirror
/// offered it first. Across the inputs, the stronger fork's apply runs in
/// the engine first in some runs and after the weaker one's in others. With equal work either tip ends selected. A node on the
/// longer fork fetches the heavier shorter one and moves to it. In the
/// reset witness the engine moves to the lighter fork once the heavier one
/// is invalidated, and back once it's reconsidered. The harness commit
/// moves the node to the two headers it committed.
#[test]
fn each_witness_offers_the_choice_it_names() {
    let stronger: String = stronger_first().mirrors[0]
        .chain
        .tip(&heavier_short().fixture())
        .hash
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut orders = HashSet::new();
    for input in inputs(8, 256) {
        for scenario in [stronger_first(), stronger_last()] {
            let ran = run(&scenario, input.clone());
            orders.insert(first_executed_apply_target(&ran.report) == Some(stronger.clone()));
        }
        for (name, scenario) in [
            ("stronger-first", stronger_first()),
            ("stronger-last", stronger_last()),
            ("three-forks", three_forks()),
            ("equal-work", equal_work()),
        ] {
            let ran = run(&scenario, input.clone());
            assert!(ran.holds_every_mirror_tip, "{name}");
            let heaviest = heaviest_mirror_tips(&scenario);
            let last = ran.selected.last().expect("a step ran");
            assert!(heaviest.contains(last), "{name}: {:?}", ran.selected);
            assert_eq!(
                heaviest.len(),
                usize::from(name == "equal-work") + 1,
                "{name}"
            );
        }

        let scenario = heavier_offered_to_longer();
        let fixture = scenario.weights.fixture();
        let ran = run(&scenario, input.clone());
        assert!(ran.holds_every_mirror_tip);
        assert_eq!(
            ran.selected.first(),
            Some(&scenario.node.tip(&fixture).hash)
        );
        assert_eq!(
            ran.selected.last(),
            Some(&scenario.mirrors[0].chain.tip(&fixture).hash)
        );

        let scenario = invalidated_then_reconsidered();
        let fixture = scenario.weights.fixture();
        let heavier = scenario.node.tip(&fixture).hash;
        let lighter = scenario.mirrors[0].chain.tip(&fixture).hash;
        let ran = run(&scenario, input.clone());
        let lighter_at = ran
            .selected
            .iter()
            .position(|tip| *tip == lighter)
            .unwrap_or_else(|| panic!("the lighter fork was selected: {:?}", ran.selected));
        assert_eq!(ran.selected.first(), Some(&heavier));
        assert_eq!(ran.selected.last(), Some(&heavier));
        assert!(lighter_at > 0);

        let scenario = harness_commit();
        let ran = run(&scenario, input);
        assert_eq!(
            ran.selected.last(),
            Some(&Chain::main(2).tip(&scenario.weights.fixture()).hash)
        );
    }
    assert_eq!(orders.len(), 2, "both arrival orders ran");
}

/// The fork-fixture witnesses are declared as the fork fixture and reach
/// settled points, where the property judges convergence and it holds. The
/// others aren't declared, so they reach none: the reset and longer-fork
/// witnesses preload the node or run controls, and the harness commit is a
/// control.
#[test]
fn the_fork_fixture_settles_on_the_heaviest_tip() {
    for input in inputs(8, 256) {
        for (name, scenario) in witnesses() {
            let declared = header_sync::fork_fixture(&scenario).is_some();
            assert_eq!(
                declared,
                matches!(
                    name,
                    "stronger-first" | "stronger-last" | "three-forks" | "equal-work"
                ),
                "{name}"
            );
            let ran = run(&scenario, input.clone());
            assert_eq!(
                ran.settled_points > 0,
                declared,
                "{name}: {}",
                ran.settled_points
            );
        }
    }
}

/// The witnesses `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-selection-<name>.json` with its Plan bytes
/// beside it in `header-selection-<name>.plan`.
fn committed_witnesses() -> [(&'static str, StorageScenario); 4] {
    [
        ("stronger-first", stronger_first()),
        ("stronger-last", stronger_last()),
        ("heavier-offered-to-longer", heavier_offered_to_longer()),
        ("harness-commit", harness_commit()),
    ]
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-selection-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-selection-{name}.{extension}"))
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
            "{name}: run `regenerate_selection_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_selection_witnesses`"
        );
        let ran = run(&scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert!(ran.holds_every_mirror_tip, "{name}");
        assert!(
            matches!(
                outcome(&ran.report, EXPECTED_CHAIN_SELECTED),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed witnesses from their builders.
#[test]
#[ignore = "rewrites the committed selection witnesses"]
fn regenerate_selection_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// The stronger-first witness's log and the property's report depend only
/// on the scenario and the plan: plan A twice, then B, then A again, gives
/// A's every time, and B's log differs.
#[test]
fn the_selection_witness_repeats_a_a_and_a_b_a() {
    let scenario = stronger_first();
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let ran = run(&scenario, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", ran.report.log.entries()),
                format!("{:?}", outcome(&ran.report, EXPECTED_CHAIN_SELECTED)),
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
            EXPECTED_CHAIN_SELECTED,
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
