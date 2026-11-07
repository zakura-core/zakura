//! Weighted fork branches and each prepare's logged work (fuzz-12 item 4).
//! In every witness the node starts at genesis, and two mirrors each hold
//! a fork from genesis on their own branch. The node prepares both forks,
//! and each prepare logs its suffix work and first header, which together
//! tell stronger, weaker and equal work apart. Which fork the node selects
//! isn't checked here.
#![cfg(zakura_sim)]

mod support;

use std::{
    cmp::Ordering,
    process::{Command, Output},
};

use support::inputs;
use tempfile::TempDir;
use zakura_chain::work::difficulty::U256;
use zakura_sim::{
    harness::{
        header_node::{Chain, Fork, Policy},
        header_sync::{grammar, Dialer, MirrorSpec, Storage, StorageScenario, Weights},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// A mirror holding `len` headers on `branch`, forked from genesis.
fn fork(branch: u8, len: u32) -> MirrorSpec {
    MirrorSpec {
        chain: Chain {
            main: 0,
            fork: Some(Fork { branch, len }),
        },
        dialer: Dialer::Mirror,
    }
}

/// The node at genesis, `mirror-0` on `len_1` headers of branch 1 and
/// `mirror-1` on `len_2` headers of branch 2, under `weights`.
fn scenario(len_1: u32, len_2: u32, weights: Weights) -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![fork(1, len_1), fork(2, len_2)],
        weights,
        policy: Policy::converging(),
        controls: Vec::new(),
    }
}

/// Equal work at equal length: four headers on each branch, both at
/// weight 1.
fn equal() -> StorageScenario {
    scenario(
        4,
        4,
        Weights {
            branch_1: 1,
            branch_2: 1,
        },
    )
}

/// A longer branch with more work: five unweighted headers on branch 1,
/// three on branch 2.
fn longer() -> StorageScenario {
    scenario(5, 3, Weights::default())
}

/// A shorter weighted branch with more work than a longer one: five
/// unweighted headers on branch 1, three at weight 2 on branch 2.
fn weighted() -> StorageScenario {
    scenario(
        5,
        3,
        Weights {
            branch_1: 0,
            branch_2: 2,
        },
    )
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> RunReport {
    let report = run_prepared::<Storage>(
        scenario,
        Storage::prepare(scenario),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    report
}

/// A prepare the node logged: its ancestor's height, its first header and
/// its work.
#[derive(Debug, PartialEq)]
struct Prepare {
    ancestor: u32,
    first: (u32, String),
    headers: u32,
    work: U256,
}

/// The node's prepares in log order, each first header once: a retried
/// prepare of the same branch is left out.
fn prepares(report: &RunReport) -> Vec<Prepare> {
    let mut seen = Vec::new();
    for entry in report.log.entries() {
        let Observation::Storage {
            instance: 0,
            fact:
                StorageFact::Submitted {
                    request:
                        StorageRequest::PrepareHeaderTarget {
                            common_ancestor,
                            headers,
                            work,
                            first,
                            ..
                        },
                    ..
                },
            ..
        } = &entry.obs
        else {
            continue;
        };
        let first = first.as_ref().expect("every prepare has headers");
        let prepare = Prepare {
            ancestor: common_ancestor.height,
            first: (first.height, first.hash.clone()),
            headers: *headers,
            work: U256::from_dec_str(work.as_deref().expect("fixture targets have work"))
                .expect("work is logged in decimal"),
        };
        if !seen.iter().any(|old: &Prepare| old.first == prepare.first) {
            seen.push(prepare);
        }
    }
    seen
}

/// What the fixture says a prepare of `mirror`'s whole fork logs: its
/// first header and its summed work.
fn expected(scenario: &StorageScenario, mirror: usize) -> Prepare {
    let fixture = scenario.weights.fixture();
    let fork = scenario.mirrors[mirror]
        .chain
        .fork
        .expect("every witness mirror holds a fork");
    let headers = fixture.extend(fixture.anchor(), fork.len, fork.branch);
    let per_header = fixture
        .target(fork.branch)
        .to_work()
        .expect("fixture targets have work")
        .as_u256();
    Prepare {
        ancestor: 0,
        first: (
            1,
            headers[0]
                .hash()
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        ),
        headers: fork.len,
        work: per_header * U256::from(fork.len),
    }
}

/// How the second fork prepared compares with the first, by work.
fn label(report: &RunReport) -> Ordering {
    let [earlier, later] = &prepares(report)[..] else {
        panic!("the node prepares each fork: {:?}", prepares(report));
    };
    later.work.cmp(&earlier.work)
}

/// In every witness, under every plan, the node prepares each mirror's
/// whole fork from genesis, and each prepare logs the work and first
/// header the fixture gives that fork.
#[test]
fn each_prepare_logs_its_forks_work() {
    for scenario in [equal(), longer(), weighted()] {
        for input in inputs(16, 256) {
            let report = run(&scenario, input);
            let mut logged = prepares(&report);
            logged.sort_by(|a, b| a.first.cmp(&b.first));
            let mut want = vec![expected(&scenario, 0), expected(&scenario, 1)];
            want.sort_by(|a, b| a.first.cmp(&b.first));
            assert_eq!(logged, want, "{scenario:?}");
        }
    }
}

/// The witnesses' forks compare as their names say: equal work at equal
/// length, more work on the longer branch, and more work on the shorter
/// weighted branch.
#[test]
fn the_witness_forks_compare_as_named() {
    let [equal_0, equal_1] = [0, 1].map(|mirror| expected(&equal(), mirror));
    assert_eq!(equal_0.headers, equal_1.headers);
    assert_eq!(equal_0.work, equal_1.work);
    let [long, short] = [0, 1].map(|mirror| expected(&longer(), mirror));
    assert!(long.headers > short.headers && long.work > short.work);
    let [long, short] = [0, 1].map(|mirror| expected(&weighted(), mirror));
    assert!(long.headers > short.headers && short.work > long.work);
}

/// Under every plan the three witnesses between them log all three labels,
/// each between two prepares with different first headers, compared in
/// the order they're logged.
#[test]
fn the_witnesses_log_stronger_weaker_and_equal_work() {
    for input in inputs(16, 256) {
        let mut labels: Vec<_> = [equal(), longer(), weighted()]
            .iter()
            .map(|scenario| label(&run(scenario, input.clone())))
            .collect();
        labels.sort();
        assert_eq!(
            labels,
            [Ordering::Less, Ordering::Equal, Ordering::Greater],
            "{input:?}"
        );
    }
}

/// Every pick and every observation of `report`, in order.
fn history(report: &RunReport) -> Vec<String> {
    report
        .picks
        .iter()
        .map(|pick| format!("{} {}", pick.step, pick.identity))
        .chain(
            report
                .log
                .entries()
                .iter()
                .map(|entry| format!("{} {:?}", entry.step, entry.obs)),
        )
        .collect()
}

/// A weighted run depends only on its scenario and plan: plan A twice
/// gives one history, and A, then B, then A gives A's history again, while
/// B's differs, so the comparison can fail.
#[test]
fn weighted_history_repeats_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    let histories: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| history(&run(&weighted(), plan.clone())))
        .collect();
    assert!(histories[0] == histories[1], "plan A twice");
    assert!(histories[0] == histories[3], "plan A, then B, then A");
    assert!(histories[0] != histories[2], "plan B differs");
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// `record` runs the weighted witness under a plan in one process, and
/// `replay` reruns its trace Exact in another.
#[test]
fn weighted_replays_exact_in_a_fresh_process() {
    let PlanInput::Bytes(plan) = &inputs(1, 256)[0] else {
        unreachable!("fuzz inputs are bytes");
    };
    let dir = TempDir::new().unwrap();
    let plan_file = dir.path().join("plan");
    let trace = dir.path().join("trace");
    std::fs::write(&plan_file, plan).unwrap();
    let recorded = zakura_sim(&[
        "record",
        "--sim",
        Storage::NAME,
        "--scenario",
        &serde_json::to_string(&weighted()).unwrap(),
        "--plan",
        plan_file.to_str().unwrap(),
        "--out",
        trace.to_str().unwrap(),
    ]);
    assert!(recorded.status.success(), "{recorded:?}");
    let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&replayed.stdout);
    assert!(
        replayed.status.success() && stdout.contains("every pick, draw and observation matched"),
        "{replayed:?}"
    );
}

/// An unweighted scenario's JSON has no `weights`, so existing scenarios
/// keep their encoding, and a weighted one round-trips.
#[test]
fn weights_appear_in_json_only_when_set() {
    let plain = serde_json::to_string(&longer()).unwrap();
    assert!(!plain.contains("weights"), "{plain}");
    let json = serde_json::to_string(&weighted()).unwrap();
    assert!(
        json.contains(r#""weights":{"branch_1":0,"branch_2":2}"#),
        "{json}"
    );
    assert_eq!(
        serde_json::from_str::<StorageScenario>(&json).unwrap(),
        weighted()
    );
}

/// A weight above 3 is refused before the run.
#[test]
#[should_panic(expected = "a weight is 0 to 3, not 4")]
fn a_weight_above_three_is_refused() {
    let mut scenario = weighted();
    scenario.weights.branch_2 = 4;
    Storage::prepare(&scenario);
}

/// Generator 2 has no branch weights, so it can't encode one.
#[test]
fn the_grammar_cant_encode_a_weight() {
    assert!(grammar::encode(&weighted()).is_err());
    assert!(grammar::encode(&longer()).is_ok());
}
