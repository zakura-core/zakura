//! Reply holds in a header-sync node run (fuzz-12 item 1): the node's
//! storage commits an apply and keeps its reply while another operation
//! completes, then delivers it, and only then does the reactor end and
//! handle the apply's call.
#![cfg(zakura_sim)]

mod support;

use std::process::{Command, Output};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{OpKind, ReplyHold, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{grammar, Dialer, MirrorSpec, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// The node at genesis syncing from `mirror-0` at A5 and serving
/// `mirror-1`, which starts at genesis too. The first apply's reply is held
/// until the node has served `mirror-1` its first page, which the node can
/// do only from the committed headers.
fn scenario() -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(5),
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                chain: Chain::default(),
                dialer: Dialer::Node,
            },
        ],
        policy: Policy {
            reply_holds: vec![ReplyHold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Completed(Select::nth(OpKind::Read, 1)),
            }],
            ..Policy::converging()
        },
        controls: Vec::new(),
    }
}

/// Runs [`scenario`] under `input`, which must end without a violation or
/// harness failure.
fn run(input: PlanInput) -> RunReport {
    let scenario = scenario();
    let report = run_prepared::<Storage>(
        &scenario,
        Storage::prepare(&scenario),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    report
}

/// The log index of the first entry `find` matches.
fn index(report: &RunReport, find: impl Fn(&Observation) -> bool) -> Option<usize> {
    report
        .log
        .entries()
        .iter()
        .position(|entry| find(&entry.obs))
}

/// Under every plan, the first apply commits, the node then completes its
/// first page read, and only after that is the apply's reply delivered to
/// its waiting caller. The reactor ends and handles the apply's call after
/// that delivery, so the reply hold held the reactor's handling, not the
/// commit.
#[test]
fn a_held_reply_outlasts_another_completion() {
    for input in inputs(16, 256) {
        let report = run(input);
        let storage = |obs: &Observation| match obs {
            Observation::Storage {
                instance: 0, fact, ..
            } => Some(fact.clone()),
            _ => None,
        };
        let (apply, call) = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match storage(&entry.obs)? {
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::ApplyHeaderTarget { .. },
                    call,
                    ..
                } => Some((id, call.expect("the reactor submits its applies"))),
                _ => None,
            })
            .expect("the node applies the mirror's headers");
        let read = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match storage(&entry.obs)? {
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::ReadHeaderPath { .. },
                    ..
                } => Some(id),
                _ => None,
            })
            .expect("the node serves mirror-1 a page");
        let at = |want: &dyn Fn(&StorageFact) -> bool| {
            index(&report, |obs| storage(obs).is_some_and(|fact| want(&fact)))
        };
        let commit = at(&|fact| matches!(fact, StorageFact::Committed { id, .. } if *id == apply))
            .expect("the held apply commits");
        let served = at(&|fact| matches!(fact, StorageFact::Completed { id, .. } if *id == read))
            .expect("the read completes");
        let delivered = at(&|fact| {
            matches!(fact, StorageFact::Completed { id, delivered: true, outcome, .. }
                if *id == apply && outcome == "engine")
        })
        .expect("the reply reaches the waiting reactor");
        let ended = index(&report, |obs| {
            matches!(obs, Observation::Reactor { event: ReactorFact::CallEnded { call: c, op, .. }, .. }
                if *c == call && *op == Some(apply))
        })
        .expect("the apply's call ends");
        let handled = index(&report, |obs| {
            matches!(obs, Observation::Reactor { event: ReactorFact::Handled { call: c, .. }, .. }
                if *c == call)
        })
        .expect("the reactor handles the apply");
        assert!(
            commit < served && served < delivered && delivered < ended && ended < handled,
            "commit {commit}, read {served}, delivery {delivered}, end {ended}, handled {handled}"
        );
        assert_eq!(
            report
                .log
                .entries()
                .iter()
                .filter(|entry| matches!(storage(&entry.obs), Some(StorageFact::Committed { .. })))
                .count(),
            1,
            "only the selected apply commits apart from its reply"
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

/// A reply-held run depends only on its scenario and plan: plan A twice
/// gives one history, and A, then B, then A gives A's history again, while
/// B's differs, so the comparison can fail.
#[test]
fn reply_hold_history_repeats_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    let histories: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| history(&run(plan.clone())))
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

/// `record` runs the reply-held scenario under a plan in one process, and
/// `replay` reruns its trace Exact in another.
#[test]
fn reply_hold_replays_exact_in_a_fresh_process() {
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
        &serde_json::to_string(&scenario()).unwrap(),
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

/// A reply hold picks only applies, so a scenario naming another kind is
/// refused before the run.
#[test]
#[should_panic(expected = "a reply hold picks only applies")]
fn a_reply_hold_of_another_kind_is_refused() {
    let mut scenario = scenario();
    scenario.policy.reply_holds[0].select = Select::nth(OpKind::Prepare, 1);
    Storage::prepare(&scenario);
}

/// Generator 2 has no reply hold, so it can't encode one.
#[test]
fn the_grammar_cant_encode_a_reply_hold() {
    assert!(grammar::encode(&scenario()).is_err());
    let mut plain = scenario();
    plain.policy.reply_holds.clear();
    assert!(grammar::encode(&plain).is_ok());
}
