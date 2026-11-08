//! hdr-9a.2: `header-sync/reset-invalidates-work` on `header-sync/storage`.
//! Each witness holds and applies, with the hdr-6 controls holding or not
//! applying beside it. `cargo xtask planted header-sync/reset-invalidates-work`
//! checks that its bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command};

use support::{byte_inputs, inputs};
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, RESET_INVALIDATES_WORK, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
            Chain, ControlAction, ControlStep, Fork, Policy, Reset,
        },
        header_sync::{Dialer, MirrorSpec, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact},
    MemorySink, Observation, PlanInput, RunReport,
};

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

/// An invalidation of the node's tip once `when` holds, then a reconsider
/// once `after` holds, so the node can still take its mirrors' chains.
fn reset_around(when: Trigger, after: Trigger) -> Vec<ControlStep> {
    vec![
        node_step(
            ControlAction::Reset(Reset::Invalidate { depth: 0 }),
            Some(when),
        ),
        node_step(ControlAction::Reset(Reset::Reconsider), Some(after)),
    ]
}

/// A hold on the `nth` operation of `kind` until the node resets.
fn held_until_reset(kind: OpKind, nth: u32) -> Hold {
    Hold {
        select: Select::nth(kind, nth),
        until: Trigger::Reset("node".to_owned()),
        then: vec![Allow::Engine],
        deadline_ms: None,
    }
}

/// The node at A3 syncing from `mirror-0` at A6. Its first locator read is
/// held until an invalidation of its tip, made once the read is submitted.
/// The read then answers in the reset's generation, so the reactor reads
/// again before it starts a round. A reconsider follows the read.
pub fn held_locator() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(6))],
        policy: Policy {
            holds: vec![held_until_reset(OpKind::Locator, 1)],
            ..Policy::converging()
        },
        controls: reset_around(
            Trigger::Submitted(Select::nth(OpKind::Locator, 1)),
            Trigger::Completed(Select::nth(OpKind::Locator, 1)),
        ),
        weights: Default::default(),
    }
}

/// [`held_locator`] for the first prepare: the round retires obsolete in
/// the unit that adopts the reset, and the late prepare is stale.
pub fn held_prepare() -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds: vec![held_until_reset(OpKind::Prepare, 1)],
            ..Policy::converging()
        },
        controls: reset_around(
            Trigger::Submitted(Select::nth(OpKind::Prepare, 1)),
            Trigger::Completed(Select::nth(OpKind::Prepare, 1)),
        ),
        ..held_locator()
    }
}

/// [`held_locator`] for the first apply, from A2: the apply runs after the
/// reset, and the engine refuses it as stale.
pub fn held_apply() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        policy: Policy {
            holds: vec![held_until_reset(OpKind::Apply, 1)],
            ..Policy::converging()
        },
        controls: reset_around(
            Trigger::Submitted(Select::nth(OpKind::Apply, 1)),
            Trigger::Completed(Select::nth(OpKind::Apply, 1)),
        ),
        ..held_locator()
    }
}

/// [`held_apply`] with the first apply's reply held instead: it commits,
/// the reset lands, and its reply is delivered in the reset's generation.
pub fn held_reply() -> StorageScenario {
    StorageScenario {
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
        ..held_apply()
    }
}

/// The node at A2 with `mirror-0` at A6 and `mirror-1` on a fork of A2
/// four headers long. Whichever mirror's apply commits first bumps the
/// generation, which retires the other mirror's round. No reset.
pub fn two_forks() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            mirror(Chain::main(6)),
            mirror(Chain {
                main: 2,
                fork: Some(Fork { branch: 1, len: 4 }),
            }),
        ],
        policy: Policy::converging(),
        controls: Vec::new(),
        weights: Default::default(),
    }
}

/// `mirror-0`'s session is replaced while its first locator read is held,
/// until the node admits the second session. The new session's round
/// starts from the old session's read, in the same generation.
pub fn locator_reuse() -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 1),
                until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted("mirror-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Relink,
            when: Some(Trigger::Submitted(Select::nth(OpKind::Locator, 1))),
        }],
        ..held_locator()
    }
}

/// The first locator read completes, and an invalidation is offered at
/// once, so under some plans the reset publishes before the reactor handles
/// the read. The reactor then starts the round under the old generation,
/// and retires it when it adopts the reset.
pub fn completion_before_adoption() -> StorageScenario {
    StorageScenario {
        policy: Policy::converging(),
        controls: reset_around(
            Trigger::Completed(Select::nth(OpKind::Locator, 1)),
            Trigger::Reset("node".to_owned()),
        ),
        ..held_locator()
    }
}

/// The node and `mirror-0` both at A3, so the node has nothing to sync, and
/// an invalidation and a reconsider of its tip at once.
pub fn quiet_reset() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(3))],
        policy: Policy::converging(),
        controls: vec![
            node_step(ControlAction::Reset(Reset::Invalidate { depth: 0 }), None),
            node_step(
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Reset("node".to_owned())),
            ),
        ],
        weights: Default::default(),
    }
}

/// Every witness, by name.
fn witnesses() -> [(&'static str, StorageScenario); 8] {
    [
        ("held-locator", held_locator()),
        ("held-prepare", held_prepare()),
        ("held-apply", held_apply()),
        ("held-reply", held_reply()),
        ("two-forks", two_forks()),
        ("locator-reuse", locator_reuse()),
        ("completion-before-adoption", completion_before_adoption()),
        ("quiet-reset", quiet_reset()),
    ]
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
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    report
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

/// The node's reactor facts, with their steps.
fn reactor(report: &RunReport) -> impl Iterator<Item = (u64, &ReactorFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { event, .. } => Some((entry.step, event)),
            _ => None,
        })
}

/// The steps of the node's `Reset` facts.
fn resets(report: &RunReport) -> Vec<u64> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Reset { .. },
                ..
            } => Some(entry.step),
            _ => None,
        })
        .collect()
}

/// Each witness holds `header-sync/reset-invalidates-work` and applies,
/// and the hdr-6 controls hold or don't apply beside it.
#[test]
fn every_witness_holds_and_applies() {
    for (name, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let report = run(&scenario, input);
            assert!(
                matches!(
                    outcome(&report, RESET_INVALIDATES_WORK),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{name}: {:?}",
                outcome(&report, RESET_INVALIDATES_WORK)
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

/// The held prepare's round retires obsolete, and its late prepare is
/// stale, so `header-sync/stale-completion-inert` applies beside the reset
/// property under some plans.
#[test]
fn the_held_prepare_retires_obsolete_and_its_prepare_is_stale() {
    let mut stale = 0;
    for input in inputs(16, 256) {
        let report = run(&held_prepare(), input);
        assert!(
            reactor(&report).any(|(_, fact)| matches!(
                fact,
                ReactorFact::Retired { reason, .. } if reason == "snapshot_obsolete"
            )),
            "the held prepare's round retires obsolete"
        );
        if matches!(
            outcome(&report, STALE_COMPLETION_INERT),
            PropertyOutcome::Held { .. }
        ) {
            stale += 1;
        }
    }
    assert!(stale > 0, "no plan's late prepare was stale");
}

/// Under some plans one mirror's commit retires the other mirror's round
/// obsolete, with no reset at all.
#[test]
fn a_commit_retires_the_other_forks_round() {
    let mut retired = 0;
    for input in inputs(16, 256) {
        let report = run(&two_forks(), input);
        assert!(resets(&report).is_empty());
        if reactor(&report).any(|(_, fact)| {
            matches!(fact, ReactorFact::Retired { reason, .. } if reason == "snapshot_obsolete")
        }) {
            retired += 1;
        }
    }
    assert!(
        retired > 0,
        "no plan's commit retired the other fork's round"
    );
}

/// Under some plans the reset lands between the locator read's completion
/// and the reactor's handling of it: the round starts after the `Reset`
/// fact, under the old generation, and retires obsolete later. Every rule
/// holds.
#[test]
fn a_round_started_before_adopting_a_reset_retires_obsolete() {
    let mut late = 0;
    for input in inputs(32, 256) {
        let report = run(&completion_before_adoption(), input);
        let Some(&reset) = resets(&report).first() else {
            continue;
        };
        let started_after = reactor(&report)
            .find(|(step, fact)| *step > reset && matches!(fact, ReactorFact::Started { .. }))
            .map(|(step, _)| step);
        let retired_after = |from: u64| {
            reactor(&report).any(|(step, fact)| {
                step >= from
                    && matches!(fact, ReactorFact::Retired { reason, .. } if reason == "snapshot_obsolete")
            })
        };
        if started_after.is_some_and(retired_after) {
            late += 1;
        }
    }
    assert!(
        late > 0,
        "no plan handled the read after the reset published"
    );
}

/// The witnesses `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-reset-<name>.json` with its Plan bytes beside
/// it in `header-reset-<name>.plan`.
fn committed_witnesses() -> [(&'static str, StorageScenario); 4] {
    [
        ("held-locator", held_locator()),
        ("held-prepare", held_prepare()),
        ("held-apply", held_apply()),
        ("quiet-reset", quiet_reset()),
    ]
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-reset-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-reset-{name}.{extension}"))
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
            "{name}: run `regenerate_reset_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_reset_witnesses`"
        );
        let report = run(&scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert!(
            matches!(
                outcome(&report, RESET_INVALIDATES_WORK),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed witnesses from their builders.
#[test]
#[ignore = "rewrites the committed reset witnesses"]
fn regenerate_reset_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// The reset property's report and the full log depend only on the
/// scenario and the plan: the held-prepare witness under plan A twice, then
/// B, then A again, gives A's every time, and B's log differs.
#[test]
fn the_reset_witness_repeats_a_a_and_a_b_a() {
    let scenario = held_prepare();
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let report = run(&scenario, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}", outcome(&report, RESET_INVALIDATES_WORK)),
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
            RESET_INVALIDATES_WORK,
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
