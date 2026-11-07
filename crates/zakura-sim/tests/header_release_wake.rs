//! hdr-7a: `header-sync/capacity-release-wakes` on `header-sync/release-wake`,
//! the wake fixtures. Two filling mirrors' rounds use the node's 500 headers
//! of body window with their prepares held, a waiting mirror's offer waits
//! for room, and a released round's leases coming back must get the waiting
//! offer's locator read to storage before the reactor goes quiet. In the
//! deferred fixture, the cached pass the release wants waits for the pass
//! budget's refill first.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, io::Write, path::Path, path::PathBuf, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::CAPACITY_RELEASE_WAKES,
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::wake::{WakeEvidence, WakeFact},
        header_sync::{Ports, ReleaseWake, WakeScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    seed,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, NullSink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The committed release input, `tests/fixtures/header-release-wake.bin`:
/// [`WakeScenario::Release`]'s scenario byte, then Plan bytes. Its run's
/// release comes from a completion drained at the top of the reactor's
/// loop, so only the budget's release wake gets the reactor back to the
/// waiting offer. `cargo xtask planted header-sync/capacity-release-wakes`
/// checks that removing that wake violates the property on it.
const RELEASE_INPUT: &[u8] = include_bytes!("fixtures/header-release-wake.bin");

/// The committed deferred input,
/// `tests/fixtures/header-release-wake-deferred.bin`:
/// [`WakeScenario::Deferred`]'s scenario byte, then Plan bytes. The
/// release's cached pass is deferred to the refill, and the planted case
/// that drops the refill deadline violates the property on it.
const DEFERRED_INPUT: &[u8] = include_bytes!("fixtures/header-release-wake-deferred.bin");

/// A committed input's file stem, its fixture, and how
/// [`regenerate_release_wake_seeds`] picks its Plan from
/// [`support::byte_inputs`]: the stream's count and longest input, then
/// the index.
type Seed = (&'static str, WakeScenario, usize, usize, usize);

/// Every committed input. The release Plan is the first tested one whose
/// release is drained at the top of the loop. Any Plan defers in the
/// deferred fixture.
const SEEDS: [Seed; 2] = [
    ("header-release-wake", WakeScenario::Release, 400, 512, 155),
    (
        "header-release-wake-deferred",
        WakeScenario::Deferred,
        16,
        256,
        0,
    ),
];

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The input [`regenerate_release_wake_seeds`] builds for `seed`: the
/// fixture's scenario byte, then its Plan.
fn generated((_, scenario, count, max_len, index): Seed) -> Vec<u8> {
    let byte = match scenario {
        WakeScenario::Release => 0,
        WakeScenario::Deferred => 1,
    };
    let mut input = vec![byte];
    input.extend(support::byte_inputs(count, max_len).swap_remove(index));
    input
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[ReleaseWake::SCENARIO_BYTES..])
}

/// `scenario`'s run under `input`, and its wake facts.
fn run(scenario: WakeScenario, input: PlanInput) -> (RunReport, Vec<(u64, WakeFact)>) {
    let ports = ReleaseWake::prepare(&scenario);
    let wake = ports.wake.clone();
    let report =
        run_prepared::<ReleaseWake>(&scenario, ports, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    (report, wake.facts())
}

/// Checks that `report`'s run opened a wake for a waiting offer, got that
/// offer's locator read to storage after it, and held
/// `header-sync/capacity-release-wakes`. Returns the facts between the
/// wake and the read.
fn wake_held<'a>(report: &RunReport, facts: &'a [(u64, WakeFact)]) -> &'a [(u64, WakeFact)] {
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let (opened, peer) = facts
        .iter()
        .enumerate()
        .find_map(|(index, (_, fact))| match fact {
            WakeFact::Opened { peer, .. } => Some((index, peer.clone())),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no release opened a wake: {facts:?}"));
    let read = facts[opened..]
        .iter()
        .position(|(_, fact)| matches!(fact, WakeFact::Submitted { peer: read } if *read == peer))
        .unwrap_or_else(|| panic!("the waiting offer's locator reached storage: {facts:?}"));
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "{outcome:?}"
    );
    &facts[opened + 1..opened + read]
}

/// Whether `facts` hold a deferred cached pass.
fn deferred(facts: &[(u64, WakeFact)]) -> bool {
    facts
        .iter()
        .any(|(_, fact)| matches!(fact, WakeFact::Deferred { .. }))
}

/// `header-sync/capacity-release-wakes` holds on `input` through the
/// registered harness, and the run reaches its trigger history. Returns
/// the facts between the wake and the waiting offer's read.
fn committed_input_holds(scenario: WakeScenario, input: &[u8]) -> Vec<(u64, WakeFact)> {
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    let result = (entry.witness)(
        &entry.scenario(&input[..ReleaseWake::SCENARIO_BYTES]),
        plan(input),
        CAPACITY_RELEASE_WAKES,
        Expect::Holds,
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
    let (report, facts) = run(scenario, plan(input));
    wake_held(&report, &facts).to_vec()
}

/// The release input holds, and no cached pass is deferred before the
/// waiting offer's read.
#[test]
fn the_release_wake_input_holds_capacity_release_wakes() {
    let between = committed_input_holds(WakeScenario::Release, RELEASE_INPUT);
    assert!(!deferred(&between), "{between:?}");
}

/// The deferred input holds, and the cached pass the release wants is
/// deferred to the refill before the waiting offer's read.
#[test]
fn the_deferred_wake_input_holds_capacity_release_wakes() {
    let between = committed_input_holds(WakeScenario::Deferred, DEFERRED_INPUT);
    assert!(deferred(&between), "{between:?}");
}

/// Each fixture opens a wake, gets the waiting offer's locator read to
/// storage and holds the property under every plan, not only the committed
/// ones. In the deferred fixture a deferral comes between every time.
#[test]
fn the_wake_fixtures_hold_under_every_plan() {
    for input in support::inputs(16, 256) {
        let (report, facts) = run(WakeScenario::Release, input.clone());
        wake_held(&report, &facts);
        let (report, facts) = run(WakeScenario::Deferred, input);
        assert!(deferred(wake_held(&report, &facts)), "{facts:?}");
    }
}

/// A fixture run without the wake declared opens no wake, so the property
/// is not applicable there.
#[test]
fn an_undeclared_fixture_opens_no_wake() {
    let scenario = WakeScenario::Release;
    let ports = Ports {
        wake: WakeEvidence::default(),
        ..ReleaseWake::prepare(&scenario)
    };
    let wake = ports.wake.clone();
    let report = run_prepared::<ReleaseWake>(
        &scenario,
        ports,
        plan(RELEASE_INPUT),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(wake.facts(), []);
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::NotApplicable)),
        "{outcome:?}"
    );
}

/// The scenario byte picks the fixture by bit 0 alone.
#[test]
fn the_scenario_byte_picks_the_fixture() {
    for byte in 0..=u8::MAX {
        let expected = if byte & 1 == 1 {
            WakeScenario::Deferred
        } else {
            WakeScenario::Release
        };
        assert_eq!(ReleaseWake::draw(&[byte]), expected, "{byte}");
    }
}

/// The wake facts, the property's report and the full log depend only on
/// the plan: the committed release plan A twice, then B, then A again,
/// gives A's every time, and B's log differs.
#[test]
fn wake_evidence_repeats_a_a_and_a_b_a() {
    let plans: [&[u8]; 2] = [&RELEASE_INPUT[ReleaseWake::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, facts) = run(WakeScenario::Release, Plan::from_fuzz(plan));
            let property = report
                .properties
                .iter()
                .find(|property| property.property == CAPACITY_RELEASE_WAKES)
                .expect("the property reports");
            (
                format!("{:?}", report.log.entries()),
                format!("{facts:?}\n{property:?}"),
            )
        })
        .collect();
    assert!(runs[0].1.contains("Opened"), "{}", runs[0].1);
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_wake_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_RELEASE_WAKE_TRACE";

thread_local! {
    /// The wake facts of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<WakeEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/release-wake`'s own run, keeping its wake facts in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: WakeScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = ReleaseWake::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.wake.clone()));
    run_prepared::<ReleaseWake>(&scenario, ports, input, sink, None)
}

/// The wake facts [`capture_run`] last kept, which must open a wake, as
/// text.
fn captured() -> String {
    let wake = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(wake.any_opened(), "{:?}", wake.facts());
    format!("{:?}\n", wake.facts())
}

/// Each committed input, recorded in this process and replayed exactly in
/// a fresh process of the same test binary, finds the same wake facts with
/// the same stamps.
#[test]
fn the_wake_witnesses_replay_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(ReleaseWake::NAME).expect("the harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        // The parent reads the wake facts from the child's stdout.
        writeln!(std::io::stdout(), "WAKE-BEGIN\n{}WAKE-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    for input in [RELEASE_INPUT, DEFERRED_INPUT] {
        let dir = TempDir::new().expect("a temporary directory");
        record(&entry, RecordInput::Input(input), dir.path()).expect("the witness records");
        let recorded = captured();
        let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "the_wake_witnesses_replay_in_a_fresh_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, dir.path())
            .output()
            .expect("the test binary runs");
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(child.status.success(), "{child:?}");
        let replayed = stdout
            .split_once("WAKE-BEGIN\n")
            .and_then(|(_, rest)| rest.split_once("WAKE-END"))
            .map(|(wake, _)| wake)
            .unwrap_or_else(|| panic!("the child prints its wake facts: {stdout}"));
        assert_eq!(replayed, recorded);
    }
}

/// Each committed input is its [`generated`] input, and runs through the
/// registered harness to a defined end that the explorer classifies clean.
#[test]
fn the_committed_inputs_are_their_plans_and_run_clean() {
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    for (seed, committed) in SEEDS.into_iter().zip([RELEASE_INPUT, DEFERRED_INPUT]) {
        let stem = seed.0;
        assert_eq!(
            committed,
            generated(seed),
            "{stem}: the committed input changed; regenerate it if that's intended"
        );
        assert_eq!(ReleaseWake::draw(committed), seed.1, "{stem}");
        let run = (entry.run_bytes)(committed, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{stem}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
    }
}

/// Rewrites the committed inputs and their sidecars. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_release_wake -- --ignored regenerate_release_wake_seeds`.
#[test]
#[ignore = "rewrites the committed inputs"]
fn regenerate_release_wake_seeds() {
    for seed in SEEDS {
        let (stem, scenario, ..) = seed;
        let input = generated(seed);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let sidecar = format!(
            "# The committed fuzz input `{stem}.bin`: the harness that runs it, the\n\
             # generator version it was built for, and its SHA-256. Its first byte\n\
             # picks the wake fixture, then come the Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_release_wake.rs\"\n\
             \n\
             [[witness]]\n\
             property = \"{CAPACITY_RELEASE_WAKES}\"\n\
             expect = \"holds\"\n",
            harness = ReleaseWake::NAME,
            generator = ReleaseWake::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(scenario).expect("scenarios serialize")
            ),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
