//! `sync/storage` (xsync-1): header sync and block sync together on one
//! node over one engine. The node starts at genesis, takes every header
//! from its mirrors' header sync, and downloads every body through block
//! sync, which follows the engine's committed views. No test injects
//! pending work.
//!
//! Each saved scenario has a committed input. It replays to its pinned end
//! record, repeats A/A and A/B/A, and replays Exact in a fresh process
//! through `zakura-sim replay`.
#![cfg(zakura_sim)]

mod support;

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        run_prepared,
        sync::{SavedSync, SyncPorts, SyncScenario, SyncStorage},
        HarnessError, Subsystem,
    },
    seed, MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
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

/// Runs a committed input as `zakura-sim record --input` does: its prefix
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

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Saved scenario `saved`'s fixture path, without an extension.
fn base(saved: SavedSync) -> PathBuf {
    fixtures().join(format!("sync-storage-{}", saved.name()))
}

/// Saved scenario `saved`'s committed input and pinned end record.
fn committed(saved: SavedSync) -> (Vec<u8>, String) {
    let base = base(saved);
    let input = fs::read(base.with_extension("bin"))
        .unwrap_or_else(|error| panic!("{}: {error}", saved.name()));
    let end = fs::read_to_string(base.with_extension("end.json"))
        .unwrap_or_else(|error| panic!("{}: {error}", saved.name()));
    (input, end.trim_end().to_owned())
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
            "header-sync/storage-operation-ids",
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

/// Each saved scenario's committed input replays to its pinned end record,
/// with both of the node's tips at the mirrors' tip unless the node shuts
/// down.
#[test]
fn saved_scenarios_replay_their_end_records() {
    for saved in SavedSync::ALL {
        let name = saved.name();
        let (input, pinned) = committed(saved);
        assert_eq!(
            SyncStorage::draw(&input[..SyncStorage::SCENARIO_BYTES]),
            saved.scenario(),
            "{name}"
        );
        let (_, lines, ports) = replay(&input);
        assert_eq!(end(&lines), pinned, "{name}");
        // A node that shuts down early stops short of the tip.
        let torn_down = matches!(
            saved,
            SavedSync::TeardownQuery | SavedSync::TeardownSubmission
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
            .map(|saved| replay(&committed(saved).0).1)
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

/// `zakura-sim record` runs each saved scenario's committed input in one
/// process, to its pinned end record, and `zakura-sim replay` reruns its
/// trace Exact in another.
#[test]
fn saved_scenarios_replay_exact_in_a_fresh_process() {
    for saved in SavedSync::ALL {
        let name = saved.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let input = base(saved).with_extension("bin");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            SyncStorage::NAME,
            "--input",
            input.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        assert_eq!(trace_end(&trace), committed(saved).1, "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// Rewrites every saved scenario's input, sidecar and end record. Each
/// input is its tag and the default Plan. Run it with `RUSTFLAGS="--cfg
/// zakura_sim" cargo test --manifest-path crates/zakura-sim/Cargo.toml
/// --test sync_harness -- --ignored regenerate_saved_scenarios`.
#[test]
#[ignore = "rewrites the committed saved scenarios"]
fn regenerate_saved_scenarios() {
    for saved in SavedSync::ALL {
        let input = saved.tag().to_vec();
        let (_, lines, _) = replay(&input);
        let base = base(saved);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let scenario = serde_json::to_value(saved.scenario()).expect("scenarios serialize");
        let mut sidecar = format!(
            "# The committed fuzz input `sync-storage-{name}.bin`: the harness that\n\
             # runs it, the generator version it was built for, and its SHA-256. Its\n\
             # first two bytes pick the saved `{name}` scenario, and\n\
             # `sync-storage-{name}.end.json` is its pinned end record.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/sync_harness.rs\"\n",
            name = saved.name(),
            harness = SyncStorage::NAME,
            generator = SyncStorage::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(&scenario),
        );
        for (property, expect) in saved.witnesses() {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"{expect}\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
        fs::write(
            base.with_extension("end.json"),
            format!("{}\n", end(&lines)),
        )
        .expect("the fixture directory exists");
    }
}
