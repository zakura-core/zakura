//! hdr-4's harness prerequisites on `header-sync/storage`: a `Shutdown` step
//! that stops the node under test on purpose, and the reactor-event triggers
//! `reservation-opened` and `session-admitted` that gate control steps and
//! holds.
#![cfg(zakura_sim)]

mod support;

use std::process::{Command, Output};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy,
        },
        header_sync::{Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> RunReport {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    report
}

/// The step that picked `identity`, if any.
fn picked(report: &RunReport, identity: &str) -> Option<u64> {
    report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .map(|pick| pick.step)
}

/// The node's reactor events, each with its step, in log order.
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

/// The ID and step of the node's first prepare submission.
fn first_prepare(report: &RunReport) -> Option<(u64, u64)> {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    },
                ..
            } => Some((*id, entry.step)),
            _ => None,
        })
}

/// The step at which the node's storage completed operation `op`, if it did.
fn completed(report: &RunReport, op: u64) -> Option<u64> {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Completed { id, .. },
                ..
            } if *id == op => Some(entry.step),
            _ => None,
        })
}

/// The default scenario, the node at genesis and `mirror-0` at A5,
/// converging, with `holds` and `controls`.
fn scenario(holds: Vec<Hold>, controls: Vec<ControlStep>) -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        controls,
        ..StorageScenario::default()
    }
}

/// The node's first prepare, held until `until`, then run on the engine.
fn hold_prepare(until: Trigger) -> Hold {
    Hold {
        select: Select::nth(OpKind::Prepare, 1),
        until,
        then: vec![Allow::Engine],
        deadline_ms: None,
    }
}

/// `action` on `peer`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    }
}

/// The shutdown witness: the node's first prepare is held until its link
/// closes, and the node shuts down once the prepare is submitted, so the
/// stop finds a prepare in flight.
fn shutdown_witness() -> StorageScenario {
    scenario(
        vec![hold_prepare(Trigger::Closed("mirror-0".to_owned()))],
        vec![step(
            "node",
            ControlAction::Shutdown,
            Trigger::Submitted(Select::nth(OpKind::Prepare, 1)),
        )],
    )
}

/// A planned stop with a prepare in flight drops that call, logs the
/// reactor's last events and passes the teardown checks, where an
/// unannounced stop would fail as `ReactorGone`. Nothing the reactor reports
/// comes after its final drain. Some plans let the reactor's request
/// timeout end the prepare first, so only some must drop it.
#[test]
fn a_shutdown_stops_the_node_and_passes_its_teardown_checks() {
    let scenario = shutdown_witness();
    let mut dropped = 0;
    for input in inputs(16, 256) {
        let report = run(&scenario, input);
        let stop = picked(&report, "peer:node:shutdown").expect("the node shuts down");
        let (prepare, _) = first_prepare(&report).expect("the node prepares");
        let ends: Vec<_> = reactor(&report)
            .filter_map(|(at, event)| match event {
                ReactorFact::CallEnded { end, op, .. } if *op == Some(prepare) => Some((at, end)),
                _ => None,
            })
            .collect();
        assert_eq!(ends.len(), 1, "the prepare ends once: {ends:?}");
        if ends[0].1 == "dropped" {
            assert!(ends[0].0 >= stop, "only the shutdown drops it");
            dropped += 1;
        }
        let last = reactor(&report).map(|(at, _)| at).last();
        assert!(
            last.is_some_and(|last| last >= stop),
            "the final drain logs the stop's events"
        );
    }
    assert!(dropped > 0, "no plan stopped the node mid-prepare");
}

/// A run cut off right after it picks `Shutdown`, before the control task
/// carries it out, fails as `ShutdownIncomplete`: the stop counts from its
/// pick, so a run can't end with a chosen stop silently untaken. The test
/// searches budgets for a run whose last step is that pick, with no drain.
#[test]
fn a_run_cut_off_at_the_shutdown_pick_fails() {
    let scenario = scenario(
        Vec::new(),
        vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Shutdown,
            when: None,
        }],
    );
    let cut = (1..64)
        .find_map(|budget| {
            let config = zakura_sim::RunConfig {
                budget,
                drain_bound: 0,
                ..zakura_sim::harness::header_sync::storage_config()
            };
            let mut sim = zakura_sim::Sim::new(config, PlanInput::Bytes(Vec::new()));
            let storage = Storage::build(&mut sim, &scenario, Storage::prepare(&scenario));
            let report = sim.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
            drop(storage);
            let last = report.picks.last().map(|pick| pick.identity.as_str());
            (last == Some("peer:node:shutdown")).then_some(report)
        })
        .expect("some budget ends the run at the shutdown pick");
    let failure = format!("{:?}", cut.end.harness);
    assert!(failure.contains("ShutdownIncomplete"), "{failure}");
}

/// A shutdown step must name the node under test, and a relink after the
/// shutdown fails the run, since the node takes no new sessions.
#[test]
fn a_shutdown_rejects_a_mirror_and_a_later_relink() {
    for controls in [
        vec![step(
            "mirror-0",
            ControlAction::Shutdown,
            Trigger::VirtualTime(0),
        )],
        vec![
            step("node", ControlAction::Shutdown, Trigger::VirtualTime(0)),
            step("mirror-0", ControlAction::Relink, Trigger::VirtualTime(0)),
        ],
    ] {
        let scenario = scenario(Vec::new(), controls);
        let ports = Storage::prepare(&scenario);
        let failure = run_prepared::<Storage>(
            &scenario,
            ports,
            inputs(1, 64).remove(0),
            &mut MemorySink::default(),
            None,
        )
        .map(|report| format!("{:?}", report.end))
        .expect_err("the step can't take effect");
        assert!(failure.to_string().contains("ControlFailed"), "{failure}");
    }
}

/// A control step can wait for the node to reserve header capacity for a
/// peer: `mirror-0`'s close comes only after the node's first call, its
/// locator read, ends, since the node reserves while it handles that reply.
/// The reactor can reach a scheduling point between the reservation and the
/// request it sends, so the close may land in between, and some plans close
/// the link before any request starts: the trigger counts the reservation,
/// not a sent request. A mirror with nothing to offer gets no reservation,
/// so its close is never offered.
#[test]
fn a_control_step_waits_for_a_reservation() {
    let close = || {
        vec![step(
            "mirror-0",
            ControlAction::Close,
            Trigger::ReservationOpened("mirror-0".to_owned()),
        )]
    };
    let scenario = scenario(Vec::new(), close());
    let mut unsent = 0;
    for input in inputs(16, 256) {
        let report = run(&scenario, input);
        let closed = picked(&report, "peer:mirror-0:close").expect("the node reserves");
        let located = reactor(&report)
            .find_map(|(at, event)| matches!(event, ReactorFact::CallEnded { .. }).then_some(at))
            .expect("the node's locator read ends");
        assert!(closed > located, "closed at {closed}, located at {located}");
        if !reactor(&report).any(|(_, event)| matches!(event, ReactorFact::Started { .. })) {
            unsent += 1;
        }
    }
    assert!(
        unsent > 0,
        "no plan closed the link between the reservation and its request"
    );

    let idle = StorageScenario {
        mirrors: vec![zakura_sim::harness::header_sync::MirrorSpec {
            chain: Chain::default(),
            dialer: zakura_sim::harness::header_sync::Dialer::Mirror,
        }],
        ..scenario
    };
    for input in inputs(4, 256) {
        let report = run(&idle, input);
        assert_eq!(picked(&report, "peer:mirror-0:close"), None);
    }
}

/// A round longer than one fair share reserves again when it continues: with
/// `mirror-0` at A300, past the 250-header share, a close waiting for the
/// node's second reservation for it is taken.
#[test]
fn a_continuation_counts_as_another_reservation() {
    let scenario = StorageScenario {
        mirrors: vec![zakura_sim::harness::header_sync::MirrorSpec {
            chain: Chain::main(300),
            dialer: zakura_sim::harness::header_sync::Dialer::Mirror,
        }],
        ..scenario(
            Vec::new(),
            vec![step(
                "mirror-0",
                ControlAction::Close,
                Trigger::Count(
                    2,
                    Box::new(Trigger::ReservationOpened("mirror-0".to_owned())),
                ),
            )],
        )
    };
    for input in inputs(4, 256) {
        let report = run(&scenario, input);
        assert!(
            picked(&report, "peer:mirror-0:close").is_some(),
            "the node reserves twice"
        );
    }
}

/// The relink witness: the node's first prepare is held until the node
/// admits `mirror-0`'s `admissions`th session, and `mirror-0` relinks once
/// the prepare is submitted.
fn relink_witness(admissions: u32) -> StorageScenario {
    scenario(
        vec![hold_prepare(Trigger::Count(
            admissions,
            Box::new(Trigger::SessionAdmitted("mirror-0".to_owned())),
        ))],
        vec![step(
            "mirror-0",
            ControlAction::Relink,
            Trigger::Submitted(Select::nth(OpKind::Prepare, 1)),
        )],
    )
}

/// A hold can wait for a session admission: the old session's prepare is
/// released once the node admits the replacement, its second session from
/// `mirror-0`, and so completes after the relink. Waiting for a third
/// admission, which never comes, keeps it held for good.
#[test]
fn a_hold_waits_for_the_replacements_admission() {
    let scenario = relink_witness(2);
    for input in inputs(16, 256) {
        let report = run(&scenario, input);
        let relinked = picked(&report, "peer:mirror-0:relink").expect("the mirror relinks");
        let (prepare, _) = first_prepare(&report).expect("the node prepares");
        let done = completed(&report, prepare).expect("the admission releases the prepare");
        assert!(
            done > relinked,
            "completed at {done}, relinked at {relinked}"
        );
    }

    let never = relink_witness(3);
    for input in inputs(4, 256) {
        let report = run(&never, input);
        let (prepare, _) = first_prepare(&report).expect("the node prepares");
        assert_eq!(completed(&report, prepare), None);
    }
}

/// The witnesses' full logs depend only on scenario and plan: plan A twice,
/// with plan B between, gives A's log both times, and B's differs.
#[test]
fn the_witnesses_repeat_a_a_and_a_b_a() {
    let plans = inputs(3, 256);
    for scenario in [shutdown_witness(), relink_witness(2)] {
        let logs: Vec<String> = [&plans[1], &plans[2], &plans[1]]
            .into_iter()
            .map(|input| format!("{:?}", run(&scenario, input.clone()).log.entries()))
            .collect();
        assert_eq!(logs[0], logs[2], "plan A, then B, then A");
        assert_ne!(logs[0], logs[1], "plan B differs");
    }
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// `record` runs each witness under a plan in one process, and `replay`
/// reruns its trace exactly in another.
#[test]
fn the_witnesses_replay_exact_in_a_fresh_process() {
    let PlanInput::Bytes(plan) = &inputs(1, 256)[0] else {
        unreachable!("fuzz inputs are bytes");
    };
    for scenario in [shutdown_witness(), relink_witness(2)] {
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
