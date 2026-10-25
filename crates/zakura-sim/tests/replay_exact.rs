//! T1c: Exact replay follows every recorded pick and draw, refuses a build
//! that isn't equivalent, and catches a run that records something else
//! (traces.md, Replay).
#![cfg(zakura_sim)]

use std::{
    cell::Cell,
    path::Path,
    rc::Rc,
    sync::OnceLock,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    action::Boundary,
    harness::{self, entry, HarnessEntry, RecordInput, Subsystem},
    property::HasView,
    trace::{
        meta::PropertyRef, replay, replay_with, Invalid, ReplayError, ReplayOptions, ReplayReport,
        Trace, TraceReader, OBSERVATIONS_FILE, TRACE_FILE,
    },
    ActionSource, Checker, Divergence, HarnessFailure, Identity, NoChecks, Observation,
    ObservationLog, RunConfig, Sim,
};

/// Records `harness` on the full fuzz `input`.
fn record(harness: &HarnessEntry, input: &[u8]) -> TempDir {
    let dir = TempDir::new().unwrap();
    harness::record(harness, RecordInput::Input(input), dir.path()).unwrap();
    dir
}

/// Records the registered harness `name` on `input`.
fn record_named(name: &str, input: &[u8]) -> TempDir {
    record(harness::find(name).unwrap(), input)
}

/// The complete trace in `dir`.
fn open(dir: &Path) -> Trace {
    TraceReader::open(dir).unwrap()
}

/// Replays the trace in `dir` with `options`, on the harness `meta.sim`
/// names.
fn replayed(dir: &Path, options: ReplayOptions) -> Result<ReplayReport, ReplayError> {
    replay(&open(dir), options)
}

/// Replays the trace in `dir` on the test harness [`Spinners`].
fn replayed_spinners(dir: &Path) -> Result<ReplayReport, ReplayError> {
    replay_with(&open(dir), &SPINNERS, ReplayOptions::default())
}

/// A [`Spinners`] trace, with a select draw in most steps.
fn spinners() -> TempDir {
    record(&SPINNERS, &[1, 0, 1, 1, 0, 1])
}

/// Rewrites `dir`'s `trace.jsonl` through `edit`, one JSON value per line.
fn edit(dir: &Path, edit: impl FnOnce(&mut Vec<Value>)) {
    let path = dir.join(TRACE_FILE);
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    edit(&mut lines);
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    std::fs::write(path, text).unwrap();
}

/// The line of step `n`, which follows `meta` and `init`.
fn step(lines: &mut [Value], n: u64) -> &mut Value {
    // Step numbers in these tests are small.
    &mut lines[n as usize + 1]
}

/// The first step of `trace` that `pred` accepts.
fn first_step(trace: &Trace, pred: impl Fn(&zakura_sim::record::StepRecord) -> bool) -> u64 {
    trace
        .steps
        .iter()
        .find(|step| pred(step))
        .expect("the trace has such a step")
        .step
}

/// The divergence a replay of `dir` hit.
fn divergence(dir: &Path) -> Divergence {
    divergence_of(replayed(dir, ReplayOptions::default()))
}

/// The divergence `result` reports.
fn divergence_of(result: Result<ReplayReport, ReplayError>) -> Divergence {
    match result {
        Err(ReplayError::ScheduleDiverged(divergence)) => divergence,
        other => panic!("expected a divergence, got {other:?}"),
    }
}

/// Fuzz input for discovery: no scenario prefix, then these Plan bytes.
const DISCOVERY: &[u8] = &[0, 1, 7, 3, 9, 1, 4, 2, 5, 5, 8, 0, 6, 2];

#[test]
fn recorded_traces_replay_exact() {
    for name in ["discovery/wait-complete", "header-sync/smoke", "race/ab-ba"] {
        let dir = record_named(name, DISCOVERY);
        let report = replayed(dir.path(), ReplayOptions::default()).unwrap();
        assert!(report.verified(), "{name}: {report:?}");
        assert_eq!(report.end, open(dir.path()).end, "{name}");
    }
}

/// `meta.build` with a different executable and `field` set to `value`.
fn other_build(dir: &Path, field: &str, value: Value) {
    edit(dir, |lines| {
        let build = &mut lines[0]["meta"]["build"];
        build["binary"] = json!("0".repeat(64));
        build[field] = value;
    });
}

#[test]
fn non_equivalent_build_refused() {
    for (field, value) in [
        ("source", json!("1".repeat(64))),
        ("rustc", json!("rustc 0.0.0 (other)")),
        ("cfg", json!(["debug_assertions", "unix"])),
    ] {
        let dir = record_named("race/lost-update", &[]);
        other_build(dir.path(), field, value);
        // No harness has this name, so a run would fail differently: the
        // build check comes before the harness is even looked up.
        edit(dir.path(), |lines| {
            lines[0]["meta"]["sim"] = json!("no/such-harness")
        });
        match replayed(dir.path(), ReplayOptions::default()) {
            Err(ReplayError::BuildMismatch { recorded, .. }) => {
                assert_eq!(recorded.binary, "0".repeat(64), "{field}");
            }
            other => panic!("{field}: expected BuildMismatch, got {other:?}"),
        }
    }
}

#[test]
fn allow_build_mismatch_never_succeeds() {
    let allow = ReplayOptions {
        allow_build_mismatch: true,
        ..ReplayOptions::default()
    };
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    // A dirty build matches only its own executable.
    other_build(dir.path(), "dirty", json!(true));
    let report = replayed(dir.path(), allow).unwrap();
    assert!(report.build_mismatch);
    assert_eq!((report.obs_mismatches, report.end_mismatch), (0, None));
    assert!(
        !report.verified(),
        "every check matched, and it still isn't a success"
    );

    let trace = open(dir.path());
    let at = first_step(&trace, |s| s.options > 1);
    edit(dir.path(), |lines| {
        step(lines, at)["pick"] = json!("task:gone.1:poll#1")
    });
    match replayed(dir.path(), allow) {
        Err(ReplayError::ScheduleDiverged(Divergence::Pick { step, .. })) => assert_eq!(step, at),
        other => panic!("expected a divergence, got {other:?}"),
    }
}

#[test]
fn diagnostic_flags_never_verify() {
    // The recording executable, so the build is equivalent: only the flags
    // differ from a plain replay.
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    assert!(replayed(dir.path(), ReplayOptions::default())
        .unwrap()
        .verified());
    for options in [
        ReplayOptions {
            allow_build_mismatch: true,
            ..ReplayOptions::default()
        },
        ReplayOptions {
            prefix: true,
            ..ReplayOptions::default()
        },
    ] {
        let report = replayed(dir.path(), options).unwrap();
        assert!(!report.build_mismatch, "{options:?}");
        assert!(!report.verified(), "{options:?}: {report:?}");
    }
}

#[test]
fn changed_pick_diverges() {
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    let trace = open(dir.path());
    let at = first_step(&trace, |s| s.options > 1);
    // Only the resume point changes. `Identity`'s `Eq` ignores it, and
    // replay mustn't.
    let pick = trace.steps[at as usize - 1].pick.clone();
    let (head, _) = pick.rsplit_once('#').unwrap();
    edit(dir.path(), |lines| {
        step(lines, at)["pick"] = json!(format!("{head}#999"))
    });
    match divergence(dir.path()) {
        Divergence::Pick { step, ready, .. } => {
            assert_eq!(step, at);
            assert!(ready.contains(&pick), "{ready:?}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn option_count_mismatch_diverges() {
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    let trace = open(dir.path());
    let at = first_step(&trace, |s| s.options > 1);
    let options = trace.steps[at as usize - 1].options;
    edit(dir.path(), |lines| {
        step(lines, at)["options"] = json!(options + 1)
    });
    assert_eq!(
        divergence(dir.path()),
        Divergence::Options {
            step: at,
            recorded: options + 1,
            found: options
        }
    );
}

#[test]
fn changed_bound_diverges() {
    let dir = spinners();
    let trace = open(dir.path());
    let at = first_step(&trace, |s| s.draws.iter().any(|d| d.bound.0 == 2));
    edit(dir.path(), |lines| {
        step(lines, at)["draws"][0]["bound"] = json!("0x3")
    });
    match divergence_of(replayed_spinners(dir.path())) {
        Divergence::Draw { step, .. } => assert_eq!(step, at),
        other => panic!("{other:?}"),
    }
}

#[test]
fn missing_init_draw_diverges() {
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    assert!(!open(dir.path()).init.unwrap().draws.is_empty());
    edit(dir.path(), |lines| lines[1]["init"]["draws"] = json!([]));
    let divergence = divergence(dir.path());
    assert_eq!(divergence.step(), 0, "{divergence:?}");
}

#[test]
fn out_of_range_draw_is_invalid() {
    let dir = spinners();
    let trace = open(dir.path());
    let at = first_step(&trace, |s| s.draws.iter().any(|d| d.bound.0 == 2));
    edit(dir.path(), |lines| {
        step(lines, at)["draws"][0]["value"] = json!("0x2")
    });
    assert!(matches!(
        replayed_spinners(dir.path()),
        Err(ReplayError::InvalidTrace(Invalid::Malformed { .. }))
    ));
}

#[test]
fn surplus_records_diverge() {
    // An extra draw on the last step.
    let dir = record_named("race/lost-update", &[]);
    let last = open(dir.path()).steps.len() as u64;
    edit(dir.path(), |lines| {
        step(lines, last)["draws"] =
            json!([{"draw": "setup:rng#9", "bound": "0x2", "value": "0x1", "by": "bytes"}]);
    });
    assert!(matches!(
        divergence(dir.path()),
        Divergence::Unreached { step, .. } if step == last
    ));

    // An extra step after the last.
    let dir = record_named("race/lost-update", &[]);
    edit(dir.path(), |lines| {
        let mut extra = step(lines, last).clone();
        extra["step"] = json!(last + 1);
        let end = lines.pop().unwrap();
        lines.push(extra);
        lines.push(end);
        let end = lines.last_mut().unwrap();
        end["end"]["steps"] = json!(last + 1);
    });
    assert!(matches!(
        divergence(dir.path()),
        Divergence::Unreached { step, .. } if step == last + 1
    ));
}

/// A run of `race/lost-update` that ends in a violation.
fn violating_race() -> TempDir {
    // Variant 0 is the planted race, and an odd byte 1 gives two workers.
    for seed in 0u8..64 {
        let input: Vec<u8> = [0, 1]
            .into_iter()
            .chain((0..32u8).map(|i| seed.wrapping_mul(31).wrapping_add(i.wrapping_mul(seed | 1))))
            .collect();
        let dir = record_named("race/lost-update", &input);
        if open(dir.path()).end.unwrap().violation.is_some() {
            return dir;
        }
    }
    panic!("some input makes the planted race fire");
}

#[test]
fn matching_violation_is_a_success() {
    let dir = violating_race();
    let report = replayed(dir.path(), ReplayOptions::default()).unwrap();
    assert!(report.verified());
    assert!(report.end.unwrap().violation.is_some());
}

#[test]
fn equivalent_build_rejects_changed_results() {
    let nondeterministic = |dir: &Path| match replayed(dir, ReplayOptions::default()) {
        Err(ReplayError::Nondeterministic { step, field }) => (step, field),
        other => panic!("expected Nondeterministic, got {other:?}"),
    };

    let dir = violating_race();
    edit(dir.path(), |lines| {
        step(lines, 2)["obs"] = json!("0000000000000000")
    });
    assert_eq!(nondeterministic(dir.path()), (2, "obs"));

    let dir = violating_race();
    let steps = open(dir.path()).end.unwrap().steps;
    edit(dir.path(), |lines| {
        lines.last_mut().unwrap()["end"]["violation"]["actual"] = json!("something else");
    });
    assert_eq!(nondeterministic(dir.path()), (steps, "end.violation"));

    let dir = violating_race();
    edit(dir.path(), |lines| {
        lines.last_mut().unwrap()["end"]["properties"][0]["outcome"] = json!("not-applicable");
    });
    assert_eq!(nondeterministic(dir.path()), (steps, "end.properties"));
}

#[test]
fn prefix_replays_and_is_never_complete() {
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    edit(dir.path(), |lines| lines.truncate(2 + 10));
    let trace = TraceReader::open_prefix(dir.path()).unwrap();
    assert!(matches!(
        replay(&trace, ReplayOptions::default()),
        Err(ReplayError::InvalidTrace(Invalid::Truncated { steps: 10 }))
    ));
    let prefix = ReplayOptions {
        prefix: true,
        ..ReplayOptions::default()
    };
    let report = replay(&trace, prefix).unwrap();
    assert_eq!(report.end, None);
    assert!(!report.complete && !report.verified(), "{report:?}");

    // A prefix still has to match.
    edit(dir.path(), |lines| {
        step(lines, 3)["obs"] = json!("0000000000000000")
    });
    let trace = TraceReader::open_prefix(dir.path()).unwrap();
    assert!(matches!(
        replay(&trace, prefix),
        Err(ReplayError::Nondeterministic { step: 3, .. })
    ));
}

#[test]
fn prefix_option_keeps_a_complete_trace_whole() {
    // Cut to ten steps, with an `end` that counts ten, so the reader takes it
    // as complete. The run goes on past it either way.
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    edit(dir.path(), |lines| {
        let mut end = lines.pop().unwrap();
        lines.truncate(2 + 10);
        end["end"]["steps"] = json!(10);
        lines.push(end);
    });
    let prefix = ReplayOptions {
        prefix: true,
        ..ReplayOptions::default()
    };
    for options in [ReplayOptions::default(), prefix] {
        match divergence_of(replayed(dir.path(), options)) {
            Divergence::PastEnd { step, .. } => assert_eq!(step, 11, "{options:?}"),
            other => panic!("{options:?}: {other:?}"),
        }
    }
}

#[test]
fn observe_writes_the_full_log() {
    let dir = record_named("discovery/wait-complete", DISCOVERY);
    let observe = ReplayOptions {
        observe: true,
        ..ReplayOptions::default()
    };
    replayed(dir.path(), observe).unwrap();
    let trace = open(dir.path());
    let log = std::fs::read_to_string(dir.path().join(OBSERVATIONS_FILE)).unwrap();
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.len(), trace.steps.len() + 1, "steps 0 to the last");
    for (n, line) in lines.iter().enumerate() {
        let head = format!(r#"{{"step":{n},"obs":"#);
        // The digest hashes the step's observations exactly as they're
        // written here.
        let obs = line.strip_prefix(&head).unwrap().strip_suffix('}').unwrap();
        let digest: String = Sha256::digest(obs.as_bytes())[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if n > 0 {
            assert_eq!(Some(digest), trace.steps[n - 1].obs, "step {n}");
        }
    }
    assert!(
        matches!(replayed(dir.path(), observe), Err(ReplayError::Io(_))),
        "the log is never overwritten"
    );
}

/// The test harnesses' empty view.
macro_rules! no_view {
    ($harness:ty) => {
        impl HasView for $harness {
            type View = ();

            /// Nothing: the harness checks nothing.
            fn view(&self) {}
        }
    };
}

/// A test harness's fixed scenario.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Fixed {}

/// A run config with small stacks and these step limits.
fn limits(budget: u64, drain_bound: u64) -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 1,
            stack_size: 64 * 1024,
            ..SchedulerConfig::default()
        },
        budget,
        drain_bound,
        ..RunConfig::default()
    }
}

/// Two tasks that loop forever, each through an unbiased two-branch
/// `select!`, which draws below 2. Task `a` builds a hash map on its third
/// pass, which draws the run's bound-0 seed. The budget runs out first, so
/// that draw lands in the drain.
struct Spinners;

/// [`Spinners`]' registry entry, which isn't in `HARNESSES`.
const SPINNERS: HarnessEntry = entry::<Spinners>();

no_view!(Spinners);

impl Subsystem for Spinners {
    const NAME: &'static str = "test/spinners";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Fixed;

    /// A short budget and drain.
    fn config(_: &Fixed) -> RunConfig {
        limits(8, 16)
    }

    /// Spawns the two spinners.
    fn build(sim: &mut Sim, _: &Fixed) -> Self {
        for label in ["a", "b"] {
            sim.scheduler().spawn_root(label, async move {
                for pass in 1u32.. {
                    zakura_rt::select! {
                        () = std::future::ready(()) => {}
                        () = std::future::ready(()) => {}
                    }
                    if label == "a" && pass == 3 {
                        let _: zakura_rt::collections::HashMap<u8, u8> = Default::default();
                    }
                    zakura_rt::task::yield_now().await;
                }
            });
        }
        Spinners
    }

    /// No checks.
    fn checker(_: &Rc<Self>, _: &Fixed) -> Box<dyn Checker> {
        Box::new(NoChecks)
    }

    /// No properties.
    fn properties(_: &Fixed) -> Vec<PropertyRef> {
        Vec::new()
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

#[test]
fn drain_picks_replay() {
    let dir = spinners();
    let trace = open(dir.path());
    let end = trace.end.clone().unwrap();
    let drained = end.drain.unwrap().steps;
    assert!(drained > 1, "{end:?}");
    let first_drain = end.steps - drained + 1;
    assert!(
        trace.steps[first_drain as usize - 1..]
            .iter()
            .any(|s| s.draws.iter().any(|d| d.bound.0 == 0)),
        "a drain step takes a bound-0 draw"
    );
    let report = replayed_spinners(dir.path()).unwrap();
    assert!(report.verified(), "{report:?}");

    // Round-robin alternates, so the next step's pick was ready here too.
    let at = first_drain;
    let next = trace.steps[at as usize].pick.clone();
    assert_ne!(next, trace.steps[at as usize - 1].pick);
    edit(dir.path(), |lines| step(lines, at)["pick"] = json!(next));
    match divergence_of(replayed_spinners(dir.path())) {
        Divergence::DrainPick { step, .. } => assert_eq!(step, at),
        other => panic!("expected a drain divergence, got {other:?}"),
    }
}

/// A source that offers one action, then two with the same identity, so a
/// run ends with a harness failure as it gathers step 2's enabled set.
struct Duplicates(Cell<bool>);

impl ActionSource for Duplicates {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.0.get() {
            out.push((Identity::peer("dup", "twice"), 0));
            out.push((Identity::peer("dup", "twice"), 1));
        } else {
            out.push((Identity::peer("dup", "once"), 0));
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        self.0.set(true);
        log.push(Observation::Fake {
            source: "dup".into(),
            event: "once".into(),
        });
    }
}

/// The [`Duplicates`] source alone.
struct DuplicateIdentity;

no_view!(DuplicateIdentity);

impl Subsystem for DuplicateIdentity {
    const NAME: &'static str = "test/duplicate-identity";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Fixed;

    /// Room for more steps than the run takes.
    fn config(_: &Fixed) -> RunConfig {
        limits(100, 10)
    }

    /// Adds the source.
    fn build(sim: &mut Sim, _: &Fixed) -> Self {
        sim.add_source(Box::new(Duplicates(Cell::new(false))));
        DuplicateIdentity
    }

    /// No checks.
    fn checker(_: &Rc<Self>, _: &Fixed) -> Box<dyn Checker> {
        Box::new(NoChecks)
    }

    /// No properties.
    fn properties(_: &Fixed) -> Vec<PropertyRef> {
        Vec::new()
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

#[test]
fn harness_failure_before_a_prefix_ends_fails() {
    let duplicates = entry::<DuplicateIdentity>();
    let dir = record(&duplicates, &[]);
    let trace = open(dir.path());
    assert_eq!(trace.steps.len(), 1);
    assert!(
        trace.end.as_ref().unwrap().harness.is_some(),
        "{:?}",
        trace.end
    );
    // The recorded harness failure replays as recorded.
    let report = replay_with(&trace, &duplicates, ReplayOptions::default()).unwrap();
    assert!(report.verified(), "{report:?}");

    // As a prefix with a step the run can't reach, it fails.
    edit(dir.path(), |lines| {
        lines.pop();
        let mut extra = step(lines, 1).clone();
        extra["step"] = json!(2);
        lines.push(extra);
    });
    let prefix = ReplayOptions {
        prefix: true,
        ..ReplayOptions::default()
    };
    let trace = TraceReader::open_prefix(dir.path()).unwrap();
    match replay_with(&trace, &duplicates, prefix) {
        Err(ReplayError::Harness(HarnessFailure::DuplicateIdentity { identity })) => {
            assert_eq!(identity, "peer:dup:twice");
        }
        other => panic!("expected the harness failure, got {other:?}"),
    }
}

/// The host clock, as nanoseconds since this process first read it.
fn host_nanos() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos()
}

/// A source that ticks three times, then reads the host clock into an
/// observation: a planted nondeterminism.
struct Clock(Cell<u32>);

impl ActionSource for Clock {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        match self.0.get() {
            0..=2 => out.push((Identity::peer("clock", &format!("tick{}", self.0.get())), 0)),
            3 => out.push((Identity::peer("clock", "read"), 0)),
            _ => {}
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let n = self.0.get();
        let event = match n {
            0..=2 => format!("tick {n}"),
            _ => format!("read {}", host_nanos()),
        };
        self.0.set(n + 1);
        log.push(Observation::Fake {
            source: "clock".into(),
            event,
        });
    }
}

/// The [`Clock`] source alone.
struct PlantedClock;

no_view!(PlantedClock);

impl Subsystem for PlantedClock {
    const NAME: &'static str = "test/planted-clock";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Fixed;

    /// Room for the four actions.
    fn config(_: &Fixed) -> RunConfig {
        limits(100, 10)
    }

    /// Adds the clock.
    fn build(sim: &mut Sim, _: &Fixed) -> Self {
        sim.add_source(Box::new(Clock(Cell::new(0))));
        PlantedClock
    }

    /// No checks.
    fn checker(_: &Rc<Self>, _: &Fixed) -> Box<dyn Checker> {
        Box::new(NoChecks)
    }

    /// No properties.
    fn properties(_: &Fixed) -> Vec<PropertyRef> {
        Vec::new()
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

#[test]
fn planted_host_clock_is_nondeterministic() {
    let clock = entry::<PlantedClock>();
    let dir = record(&clock, &[]);
    // Each read is later than the last, so the replay's differs.
    std::thread::sleep(Duration::from_micros(10));
    match replay_with(&open(dir.path()), &clock, ReplayOptions::default()) {
        Err(ReplayError::Nondeterministic { step, field }) => assert_eq!((step, field), (4, "obs")),
        other => panic!("expected Nondeterministic, got {other:?}"),
    }
}
