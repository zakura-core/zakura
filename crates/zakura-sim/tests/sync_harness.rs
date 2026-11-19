//! `sync/storage` (xsync-1): header sync and block sync together on one
//! node over one engine. The node starts at genesis, takes every header
//! from its mirrors' header sync, and downloads every body through block
//! sync, which follows the engine's committed views. No test injects
//! pending work.
//!
//! Each saved scenario is a seed. It runs to its step budget holding its
//! witnesses, repeats A/A and A/B/A, and replays Exact in a fresh process
//! through `zakura-sim replay`.
#![cfg(zakura_sim)]

mod support;

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    domain::cross::HEADER_REORG_RETIRES_BLOCK_WORK,
    harness::{
        run_prepared,
        sync::{SavedSync, SyncPorts, SyncScenario, SyncStorage},
        HarnessError, Subsystem,
    },
    observe::BlockSyncFact,
    seed, MemorySink, Observation, ObservationLog, Outcome, Plan, PlanInput, PropertyOutcome,
    RunReport,
};

/// Runs `scenario` under `input`, and returns the report with its records
/// and the ports, which outlive the run. Fails the test on a harness
/// failure or an active property's violation.
fn run(scenario: &SyncScenario, input: PlanInput) -> (RunReport, Vec<String>, SyncPorts) {
    let ports = SyncStorage::prepare(scenario);
    let mut sink = MemorySink::default();
    let report = match run_prepared::<SyncStorage>(scenario, ports.clone(), input, &mut sink, None)
    {
        Ok(report) => report,
        Err(HarnessError::Run { failure, report }) => {
            panic!("{failure}: {:?}", report.end)
        }
        Err(error) => panic!("{error}"),
    };
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, sink.lines(), ports)
}

/// Runs a full fuzz input as `zakura-sim record --seed` does: its prefix
/// picks the scenario, and the rest is the Plan.
fn replay(input: &[u8]) -> (RunReport, Vec<String>, SyncPorts) {
    let (prefix, plan) = input.split_at(SyncStorage::SCENARIO_BYTES);
    run(&SyncStorage::draw(prefix), Plan::from_fuzz(plan))
}

/// Panics on any violated property, and returns the ones that held at
/// least once.
fn held(report: &RunReport) -> Vec<String> {
    let mut held = Vec::new();
    for property in &report.properties {
        match &property.outcome {
            PropertyOutcome::Violated { .. } => {
                panic!("{} was violated: {:?}", property.property, property.outcome)
            }
            PropertyOutcome::Held { applicable } if *applicable > 0 => {
                held.push(property.property.clone());
            }
            _ => {}
        }
    }
    held
}

/// A run's end record: its last line.
fn end(lines: &[String]) -> &str {
    lines.last().expect("a run writes its end record")
}

/// xsync-1: the node starts at genesis between two mirrors, one dialing it
/// and one it dials. Its header sync takes the mirrors' eight headers, and
/// its block sync, woken only by the engine's committed views, downloads
/// every body, so its header tip and verified tip both reach the mirrors'
/// tip.
#[test]
fn honest_sync_reaches_the_tip_through_both_services() {
    let scenario = SavedSync::Honest.scenario();
    for input in inputs(2, 256) {
        let (report, _, ports) = run(&scenario, input);
        let tip = ports.chain.last().expect("the chain has blocks").frontier();
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        let held = held(&report);
        for property in [
            "block-sync/storage-operation-ids",
            "block-sync/submit-only-requested",
        ] {
            assert!(held.iter().any(|id| id == property), "{property}: {held:?}");
        }
    }
}

/// xsync-1: the mirrors switch to a fork from height 4 once the node has
/// admitted four bodies. Only the node's header sync learns of it, from its
/// mirrors. The engine's next committed view reaches block sync, which
/// downloads the fork's bodies, so both tips reach the fork's tip.
#[test]
fn a_header_reorg_reaches_block_sync_through_the_engine() {
    let scenario = SavedSync::Reorg.scenario();
    for input in inputs(2, 256) {
        let (report, _, ports) = run(&scenario, input);
        let tip = ports.mirror_tip();
        assert_eq!(tip.height.0, 12, "the fork outgrows the old tip");
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        held(&report);
    }
}

/// Each saved scenario's seed, its tag and the empty Plan, runs to its step
/// budget with no property violated, its drain runs dry, and cleanup finds
/// no panic, failed unwind or leaked task. Each of its witnesses applies
/// and holds, and both of the node's tips reach the mirrors' tip unless the
/// node shuts down.
#[test]
fn saved_scenarios_run_to_their_budget_holding_their_witnesses() {
    for saved in SavedSync::ALL {
        let name = saved.name();
        let input = saved.tag();
        assert_eq!(SyncStorage::draw(&input), saved.scenario(), "{name}");
        let (report, _, ports) = replay(&input);
        assert_eq!(
            report.end.outcome,
            Outcome::Budget,
            "{name}: {:?}",
            report.end
        );
        assert!(
            report
                .end
                .drain
                .as_ref()
                .is_some_and(|drain| !drain.exhausted),
            "{name}: {:?}",
            report.end
        );
        let cleanup = &report.cleanup;
        assert_eq!(
            (cleanup.panicked, cleanup.unwind_failures, cleanup.leaked),
            (0, 0, 0),
            "{name}: {cleanup:?}"
        );
        let held = held(&report);
        for (property, _) in saved.witnesses() {
            assert!(
                held.iter().any(|id| id == property),
                "{name}: {property}: {held:?}"
            );
        }
        // A node that shuts down early stops short of the tip.
        let torn_down = matches!(
            saved,
            SavedSync::TeardownQuery | SavedSync::TeardownSubmission | SavedSync::TeardownHeaders
        );
        let tip = ports.mirror_tip();
        assert!(torn_down || ports.tips() == (tip, tip), "{name}");
    }
}

/// A/A and A/B/A: each saved scenario, run twice, then after the next one,
/// gives the same records every time. The one between gives other records,
/// so the comparison can fail.
#[test]
fn saved_scenarios_repeat_a_a_and_a_b_a() {
    for (index, a) in SavedSync::ALL.into_iter().enumerate() {
        let b = SavedSync::ALL[(index + 1) % SavedSync::ALL.len()];
        let runs: Vec<Vec<String>> = [a, a, b, a]
            .into_iter()
            .map(|saved| replay(&saved.tag()).1)
            .collect();
        assert_eq!(runs[0], runs[1], "{} twice", a.name());
        assert_eq!(
            runs[0],
            runs[3],
            "{}, then {}, then again",
            a.name(),
            b.name()
        );
        assert_ne!(runs[0], runs[2], "{} differs from {}", b.name(), a.name());
    }
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// The end record of the trace in `dir`: its `trace.jsonl`'s last line.
fn trace_end(dir: &Path) -> String {
    let trace = fs::read_to_string(dir.join(zakura_sim::trace::TRACE_FILE)).unwrap();
    trace.lines().last().unwrap().to_owned()
}

/// `zakura-sim record --seed` runs each saved scenario's seed in another
/// process to the end record this process's run gives, and `zakura-sim
/// replay` reruns its trace Exact in a third.
#[test]
fn saved_scenarios_replay_exact_in_a_fresh_process() {
    for saved in SavedSync::ALL {
        let name = saved.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let recorded = zakura_sim(&[
            "record",
            "--seed",
            &format!("sync-storage-{name}"),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let (_, lines, _) = replay(&saved.tag());
        assert_eq!(trace_end(&trace), end(&lines), "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// The seed of `sync/header-reorg-retires-block-work`'s planted case: the
/// saved `reorg` scenario under a fuzz Plan whose run delivers an old-epoch
/// body after the sequencer applied the new epoch.
const REORG_STALE_BODY: &str = "sync-storage-reorg-stale-body";

/// Checks that `input` fills the stale-body input's role, or returns why it
/// falls short. Its run must hold every property, apply
/// `sync/header-reorg-retires-block-work`, and have the sequencer refuse an
/// older epoch's body after it applied a newer one. That refused body is
/// the one `accept-stale-scope-bodies.patch` buffers instead.
fn stale_body_shortfall(input: &[u8]) -> Result<(), String> {
    let (prefix, plan) = input.split_at(SyncStorage::SCENARIO_BYTES);
    let scenario = SyncStorage::draw(prefix);
    let ports = SyncStorage::prepare(&scenario);
    let mut sink = MemorySink::default();
    let report = run_prepared::<SyncStorage>(
        &scenario,
        ports.clone(),
        Plan::from_fuzz(plan),
        &mut sink,
        None,
    )
    .map_err(|error| error.to_string())?;
    if let Some(violation) = &report.end.violation {
        return Err(format!("the run violated {violation:?}"));
    }
    if let Some(violated) = report
        .properties
        .iter()
        .find(|property| matches!(property.outcome, PropertyOutcome::Violated { .. }))
    {
        return Err(format!("{} was violated", violated.property));
    }
    let applied = report.properties.iter().any(|property| {
        property.property == HEADER_REORG_RETIRES_BLOCK_WORK
            && matches!(property.outcome, PropertyOutcome::Held { applicable } if applicable > 0)
    });
    if !applied {
        return Err(format!("{HEADER_REORG_RETIRES_BLOCK_WORK} never applied"));
    }
    if !ports.cross.borrow().saw_stale_body_refused {
        return Err("the sequencer refused no old-epoch body under a newer epoch".to_owned());
    }
    Ok(())
}

/// The stale-body seed runs the saved `reorg` scenario and still makes the
/// sequencer refuse an old-epoch body, so a block-sync change that moves it
/// off that order fails here instead of weakening the planted case. When
/// it does, search [`zakura_sim::seed::draws::byte_inputs`] for a Plan
/// that passes [`stale_body_shortfall`] and build the seed from it.
#[test]
fn the_stale_body_input_refuses_an_old_epoch_body() {
    let input = seed::find(REORG_STALE_BODY)
        .expect("the seed is registered")
        .input()
        .to_vec();
    assert_eq!(
        SyncStorage::draw(&input[..SyncStorage::SCENARIO_BYTES]),
        SavedSync::Reorg.scenario()
    );
    if let Err(shortfall) = stale_body_shortfall(&input) {
        panic!("{shortfall}");
    }
}

/// Whether the sequencer applied a new body-work epoch while the work queue
/// still held an older epoch's taken work above the new verified tip: work
/// the reset that comes with the apply retires. The queue reports that
/// reset after the sequencer reports the apply, so its last snapshot before
/// the apply is the work the reset finds.
fn reset_finds_stale_work(log: &ObservationLog) -> bool {
    let mut queue = &Vec::new();
    log.entries().iter().any(|entry| match &entry.obs {
        Observation::BlockSync {
            event: BlockSyncFact::WorkQueue { items },
            ..
        } => {
            queue = items;
            false
        }
        Observation::BlockSync {
            event:
                BlockSyncFact::EpochApplied {
                    epoch,
                    verified_tip,
                },
            ..
        } => queue.iter().any(|item| {
            item.height > *verified_tip
                && item
                    .owner
                    .as_ref()
                    .and_then(|owner| owner.body_work_epoch)
                    .is_some_and(|own| own < *epoch)
        }),
        _ => false,
    })
}

/// `omit-work-reset`'s seed, the saved `reorg-request` scenario, still has
/// the sequencer apply the new epoch while the work queue holds the old
/// epoch's taken work above the verified tip, which the held range read
/// keeps there. Without that work the case's bypass leaves nothing behind,
/// as #2321 left `sync-storage-reorg.bin`.
#[test]
fn the_reorg_request_input_resets_old_epoch_work() {
    let (report, _, _) = replay(&SavedSync::ReorgRequest.tag());
    assert!(reset_finds_stale_work(&report.log), "{:?}", report.end);
}
