//! Resets, releases and storage stops in a header-sync node run (fuzz-12
//! items 2 and 3): an operator invalidation across a held prepare and a
//! held reply, resets with nothing to do, a release the Plan places, and a
//! storage stop after the node's shutdown.
#![cfg(zakura_sim)]

mod support;

use std::process::{Command, Output};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy, Reset,
        },
        header_sync::{grammar, Dialer, MirrorSpec, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    Identity, MemorySink, Observation, PlanInput, RunReport,
};

/// A control step on the node under test.
fn node_step(action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: "node".to_owned(),
        action,
        when,
    }
}

/// A mirror holding `chain`.
fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec { chain, dialer }
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

/// The node's storage facts, with their steps, in log order.
fn facts(report: &RunReport) -> Vec<(u64, StorageFact)> {
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

/// The ID of the first operation `want` matches at its submission.
fn submitted(report: &RunReport, want: impl Fn(&StorageRequest) -> bool) -> Option<u64> {
    facts(report).into_iter().find_map(|(_, fact)| match fact {
        StorageFact::Submitted { id, request, .. } if want(&request) => Some(id),
        _ => None,
    })
}

/// Operation `id`'s `Completed` fact, with its step.
fn completed(report: &RunReport, id: u64) -> Option<(u64, StorageFact)> {
    facts(report)
        .into_iter()
        .find(|(_, fact)| matches!(fact, StorageFact::Completed { id: done, .. } if *done == id))
}

/// Each `Reset` fact's kind, target and generation, with its step.
fn resets(report: &RunReport) -> Vec<(u64, String, String, u64)> {
    facts(report)
        .into_iter()
        .filter_map(|(step, fact)| match fact {
            StorageFact::Reset {
                kind,
                target,
                generation,
            } => Some((step, kind, target, generation)),
            _ => None,
        })
        .collect()
}

/// The steps at which the node's control step `action` was picked.
fn picked(report: &RunReport, action: &str) -> Vec<u64> {
    let identity = Identity::peer("node", action).to_string();
    report
        .picks
        .iter()
        .filter(|pick| pick.identity == identity)
        .map(|pick| pick.step)
        .collect()
}

/// The node at A3 syncing from `mirror-0` at A6. The first prepare is held
/// until the node resets, and the reset invalidates the node's tip once
/// that prepare is submitted. Once the prepare completes, a reconsider
/// restores the tip, so the node can still take the mirror's chain.
fn reset_held_prepare() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(6), Dialer::Mirror)],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::Reset("node".to_owned()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![
            node_step(
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::Submitted(Select::nth(OpKind::Prepare, 1))),
            ),
            node_step(
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Completed(Select::nth(OpKind::Prepare, 1))),
            ),
        ],
    }
}

/// A prepare held across an invalidation of the node's tip completes after
/// it, and its `Completed` fact carries the generation the reset made. The
/// reconsider names the same header, and the node then reaches the
/// mirror's tip, which descends from it.
#[test]
fn a_held_prepare_completes_in_the_reset_generation() {
    let scenario = reset_held_prepare();
    for input in inputs(8, 256) {
        let report = run(&scenario, input);
        let resets = resets(&report);
        let [(invalidated, kind, target, generation), (reconsidered, again, same, _)] =
            resets.as_slice()
        else {
            panic!("one invalidation and one reconsider: {resets:?}");
        };
        assert_eq!(
            (kind.as_str(), again.as_str()),
            ("invalidate", "reconsider")
        );
        assert_eq!(target, same, "the reconsider restores the invalidated tip");
        let prepare = submitted(&report, |request| {
            matches!(request, StorageRequest::PrepareHeaderTarget { .. })
        })
        .expect("the node prepares the mirror's headers");
        let (done, fact) = completed(&report, prepare).expect("the held prepare completes");
        let StorageFact::Completed { generation: at, .. } = fact else {
            unreachable!("`completed` finds completions");
        };
        assert!(*invalidated <= done && done <= *reconsidered);
        assert_eq!(
            at, *generation,
            "the prepare completes in the reset generation"
        );
        assert!(
            report.end.violation.is_none(),
            "the node still reaches the mirror's tip"
        );
    }
}

/// An apply whose reply is held across an invalidation: its `Committed`
/// carries the commit's generation and its `Completed` the reset's.
#[test]
fn a_held_reply_is_delivered_in_the_reset_generation() {
    let scenario = StorageScenario {
        node: Chain::main(1),
        mirrors: vec![mirror(Chain::main(5), Dialer::Mirror)],
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
    };
    for input in inputs(8, 256) {
        let report = run(&scenario, input);
        let apply = submitted(&report, |request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { .. })
        })
        .expect("the node applies the mirror's headers");
        let committed = facts(&report)
            .into_iter()
            .find_map(|(step, fact)| match fact {
                StorageFact::Committed { id, generation, .. } if id == apply => {
                    Some((step, generation))
                }
                _ => None,
            });
        let (commit, at_commit) = committed.expect("the apply commits");
        let [(reset, _, _, at_reset)] = resets(&report)[..] else {
            panic!("one invalidation");
        };
        let (delivered, fact) = completed(&report, apply).expect("the reply is delivered");
        let StorageFact::Completed { generation, .. } = fact else {
            unreachable!("`completed` finds completions");
        };
        assert!(commit <= reset && reset <= delivered);
        assert_ne!(at_commit, at_reset);
        assert_eq!(generation, at_reset);
    }
}

/// With the node at the anchor and nothing to sync, an invalidation and a
/// reconsider are both taken but do nothing, publish no `Reset` fact, and
/// count for no `reset` trigger, so the hold on it never opens.
#[test]
fn resets_with_nothing_to_do_publish_nothing() {
    let scenario = StorageScenario {
        node: Chain::default(),
        mirrors: vec![mirror(Chain::default(), Dialer::Mirror)],
        policy: Policy::converging(),
        controls: vec![
            node_step(ControlAction::Reset(Reset::Invalidate { depth: 0 }), None),
            node_step(ControlAction::Reset(Reset::Reconsider), None),
            node_step(
                ControlAction::Close,
                Some(Trigger::Reset("node".to_owned())),
            ),
        ],
    };
    for input in inputs(4, 256) {
        let report = run(&scenario, input);
        assert_eq!(picked(&report, "reset").len(), 2);
        assert!(picked(&report, "close").is_empty());
        assert!(resets(&report).is_empty());
    }
}

/// The node at A5 serving `mirror-1` at genesis. Its first page read is
/// held until a `Release`, which the Plan places once the first acquire has
/// completed.
fn released_read() -> StorageScenario {
    StorageScenario {
        node: Chain::main(5),
        mirrors: vec![mirror(Chain::default(), Dialer::Node)],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Read, 1),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![node_step(
            ControlAction::Release { hold: 0 },
            Some(Trigger::Completed(Select::nth(OpKind::Acquire, 1))),
        )],
    }
}

/// A read held until a release completes only after the release's pick,
/// which comes after the acquire's completion.
#[test]
fn a_released_read_completes_after_its_release() {
    let scenario = released_read();
    for input in inputs(8, 256) {
        let report = run(&scenario, input);
        let [release] = picked(&report, "release")[..] else {
            panic!("one release");
        };
        let acquire = submitted(&report, |request| {
            matches!(request, StorageRequest::AcquireHeaderPath { .. })
        })
        .expect("the node serves an acquire");
        let read = submitted(&report, |request| {
            matches!(request, StorageRequest::ReadHeaderPath { .. })
        })
        .expect("the node serves a read");
        let (acquired, _) = completed(&report, acquire).expect("the acquire completes");
        let (served, _) = completed(&report, read).expect("the read completes");
        assert!(acquired <= release && release < served);
    }
}

/// The node shuts down with its first apply held, then its storage stops.
/// The logged `shut-down` fact lists the apply as dropped, after the
/// reactor dropped its call.
#[test]
fn a_storage_stop_logs_the_held_apply_as_dropped() {
    let scenario = StorageScenario {
        node: Chain::default(),
        mirrors: vec![mirror(Chain::main(5), Dialer::Mirror)],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![
            node_step(
                ControlAction::Shutdown,
                Some(Trigger::Submitted(Select::nth(OpKind::Apply, 1))),
            ),
            node_step(ControlAction::StopStorage, None),
        ],
    };
    for input in inputs(8, 256) {
        let report = run(&scenario, input);
        let apply = submitted(&report, |request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { .. })
        })
        .expect("the node submits an apply");
        let [stop] = picked(&report, "stop-storage")[..] else {
            panic!("one storage stop");
        };
        let facts = facts(&report);
        let dropped = facts
            .iter()
            .position(|(_, fact)| matches!(fact, StorageFact::CallerDropped { id } if *id == apply))
            .expect("the reactor drops the apply's call");
        let (shut, fact) = facts
            .iter()
            .enumerate()
            .find_map(|(index, (_, fact))| {
                matches!(fact, StorageFact::ShutDown { .. }).then_some((index, fact))
            })
            .expect("the shutdown is logged in the run");
        let StorageFact::ShutDown {
            dropped: ops,
            callers_alive,
            ..
        } = fact
        else {
            unreachable!("found a shutdown");
        };
        assert!(dropped < shut);
        assert_eq!(facts[shut].0, stop);
        assert!(ops.contains(&apply));
        assert!(
            callers_alive.is_empty(),
            "the stopped reactor holds no call"
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

/// The reset and release witnesses depend only on their scenario and plan:
/// A twice, then A, B, A, give A's history each time, and B's differs.
#[test]
fn reset_and_release_histories_repeat_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    for scenario in [reset_held_prepare(), released_read()] {
        let histories: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
            .into_iter()
            .map(|plan| history(&run(&scenario, plan.clone())))
            .collect();
        assert!(histories[0] == histories[1], "plan A twice");
        assert!(histories[0] == histories[3], "plan A, then B, then A");
        assert!(histories[0] != histories[2], "plan B differs");
    }
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// `record` runs the reset and release witnesses under a plan in one
/// process, and `replay` reruns each trace Exact in another.
#[test]
fn reset_and_release_replay_exact_in_a_fresh_process() {
    let PlanInput::Bytes(plan) = &inputs(1, 256)[0] else {
        unreachable!("fuzz inputs are bytes");
    };
    for scenario in [reset_held_prepare(), released_read()] {
        let dir = TempDir::new().unwrap();
        let plan_file = dir.path().join("plan");
        let trace = dir.path().join("trace");
        std::fs::write(&plan_file, plan).unwrap();
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &serde_json::to_string(&scenario).unwrap(),
            "--plan",
            plan_file.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{recorded:?}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{replayed:?}"
        );
    }
}

/// `released` holds only in a hold's own trigger, so a control step that
/// waits for it is refused before the run.
#[test]
#[should_panic(expected = "`released` holds only in a hold's own trigger")]
fn released_outside_a_hold_is_refused() {
    let mut scenario = released_read();
    scenario.controls[0].when = Some(Trigger::Released);
    Storage::prepare(&scenario);
}

/// A release must name one of the policy's holds.
#[test]
#[should_panic(expected = "releases a hold the policy's 1 holds don't have")]
fn a_release_of_a_missing_hold_is_refused() {
    let mut scenario = released_read();
    scenario.controls[0].action = ControlAction::Release { hold: 1 };
    Storage::prepare(&scenario);
}

/// Generator 2 encodes none of the new control steps or `released`.
#[test]
fn the_grammar_cant_encode_resets_releases_or_stops() {
    assert!(grammar::encode(&reset_held_prepare()).is_err());
    assert!(grammar::encode(&released_read()).is_err());
    let mut stop = released_read();
    stop.policy.holds.clear();
    stop.controls = vec![node_step(ControlAction::StopStorage, None)];
    assert!(grammar::encode(&stop).is_err());
}
