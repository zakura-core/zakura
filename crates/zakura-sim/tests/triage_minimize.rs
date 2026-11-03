//! E4a with T2b: `triage` minimizes a confirmed `Active` failure with the
//! minimizer `Children::minimize` gives, and archives the best candidate in
//! a bundle beside the failure's bundle only if the minimizer's fresh
//! confirmations hold to that bundle.
//!
//! The minimizer here stands in for `cargo xtask sim-minimize`. It's this
//! test binary, which confirms the input and one candidate with
//! `trace::confirm`, as `zakura-sim confirm` does, and writes the
//! minimizer's report. It runs no `tmin`. Its mode picks what it gets
//! wrong.
#![cfg(zakura_sim)]

use std::{
    cell::Cell,
    fs,
    path::{Path, PathBuf},
    process::Command,
    rc::Rc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::TempDir;
use zakura_scheduler::{SchedulerConfig, Status as SchedulerStatus};
use zakura_sim::{
    explore::{
        triage::{
            bundles, triage, verify, verify_bundle, Children, Minimization, SimChildren,
            SimMinimize, TriageOptions, TriageStatus, Triaged, BUNDLE_INPUT_FILE, CHECKSUMS_FILE,
            MINIMIZED_FILE, MINIMIZER_DIR, RUNS_DIR, STAGING_DIR, TRIAGE_FILE,
        },
        Bucket,
    },
    harness::{entry, record, HarnessEntry, RecordInput, Subsystem},
    property::{
        Assume, AssumeRecord, Assumptions, CheckCtx, HasView, Kind, Property, PropertyId, Source,
        Sources, Status, Verdict,
    },
    trace::{confirm, replay_with, ReplayOptions, TraceReader, CONFIRMATION_FILE},
    Observation, ObservationLog, RunConfig, Sim,
};

/// The first planted property.
const A: &str = "test/triage-minimize/a";

/// The second planted property.
const B: &str = "test/triage-minimize/b";

/// Scenario byte 0: break [`A`].
const FAULT_A: u8 = 0;
/// Scenario byte 0: break [`B`].
const FAULT_B: u8 = 1;
/// Scenario byte 0: break nothing.
const FAULT_NONE: u8 = 2;
/// Scenario byte 0: break [`A`] beside a task whose name the run refuses,
/// a harness failure.
const FAULT_HARNESS: u8 = 3;

/// The planted harness's scenario.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    /// What the `fault` task breaks.
    fault: u8,
    /// The declared delivery bound's parameter.
    max_delay_ms: u8,
    /// Whether a `trigger` task runs first: the trigger history.
    trigger: bool,
}

/// Two planted `Active` properties with a trigger history.
struct Planted {
    /// Whether [`A`] and [`B`] are broken.
    broken: Rc<(Cell<bool>, Cell<bool>)>,
}

impl HasView for Planted {
    type View = (bool, bool);

    fn view(&self) -> (bool, bool) {
        (self.broken.0.get(), self.broken.1.get())
    }
}

impl Subsystem for Planted {
    const NAME: &'static str = "test/triage-minimize";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 3;
    type Scenario = Scenario;
    type Prepared = ();

    /// One worker, small stacks, short limits.
    fn config(_: &Scenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 100,
            drain_bound: 20,
            ..RunConfig::default()
        }
    }

    fn prepare(_: &Scenario) {}

    /// Spawns `trigger` if the scenario asks, then `fault`.
    fn build(sim: &mut Sim, scenario: &Scenario, (): ()) -> Self {
        let broken = Rc::new((Cell::new(false), Cell::new(false)));
        if scenario.trigger {
            sim.scheduler().spawn_root("trigger", async {});
        }
        if scenario.fault == FAULT_HARNESS {
            sim.scheduler().spawn_root("bad:name", async {});
        }
        let fault = scenario.fault;
        let state = Rc::clone(&broken);
        sim.scheduler().spawn_root("fault", async move {
            match fault {
                FAULT_A | FAULT_HARNESS => state.0.set(true),
                FAULT_B => state.1.set(true),
                _ => {}
            }
        });
        Planted { broken }
    }

    fn properties() -> Vec<Property<Self>> {
        vec![planted(A, check_a), planted(B, check_b)]
    }

    fn sources() -> Sources {
        Sources::observed(&[Source::View, Source::Schedule])
    }

    /// Declares the delivery bound with the scenario's parameter.
    fn assumptions(scenario: &Scenario) -> Assumptions<Self> {
        Assumptions::declare(vec![AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: json!({ "max_delay_ms": scenario.max_delay_ms }),
        }])
    }

    /// Byte 0 is the fault, byte 1 the delay parameter, and byte 2's low
    /// bit runs `trigger`.
    fn draw(prefix: &[u8]) -> Scenario {
        Scenario {
            fault: prefix[0],
            max_delay_ms: prefix[1],
            trigger: prefix[2] & 1 == 1,
        }
    }
}

/// An `Active` planted property `id` with `check`, whose trigger history
/// is a run of the `trigger` task.
fn planted(
    id: &'static str,
    check: fn(&mut CheckCtx<'_, Planted>) -> Verdict,
) -> Property<Planted> {
    Property {
        id: PropertyId::new(id),
        claim: "the fault task breaks nothing",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View, Source::Schedule],
        status: Status::Active,
        check,
        witness: Some(triggered),
    }
}

fn check_a(cx: &mut CheckCtx<'_, Planted>) -> Verdict {
    check(cx, |(a, _)| a)
}

fn check_b(cx: &mut CheckCtx<'_, Planted>) -> Verdict {
    check(cx, |(_, b)| b)
}

/// Violated once `broke` holds on the view, after a step that leaves no
/// task running.
fn check(cx: &mut CheckCtx<'_, Planted>, broke: fn((bool, bool)) -> bool) -> Verdict {
    if cx.status == SchedulerStatus::Running {
        return Verdict::NotApplicable;
    }
    if broke(cx.view()) {
        cx.violated("nothing broken", "the fault task broke it")
    } else {
        Verdict::Holds
    }
}

/// True if the `trigger` task ran to its end.
fn triggered(log: &ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(&entry.obs, Observation::TaskExited { task, .. } if task.starts_with("trigger"))
    })
}

static PLANTED: HarnessEntry = entry::<Planted>();

/// A failure of [`A`]: fault [`A`], a 7 ms delay bound, the trigger, and
/// Plan bytes a reduction may drop.
const ORIGINAL: [u8; 6] = [FAULT_A, 7, 1, 9, 9, 9];

/// What the stand-in minimizer does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Keeps the scenario prefix, which reproduces [`A`].
    Minimized,
    /// Keeps no candidate: `original-kept`, exit 1.
    Kept,
    /// Aborts before it writes a report.
    Abort,
    /// Reports on another input than the one it got.
    OtherInput,
    /// Confirms the original on another build.
    OtherBuild,
    /// Reports a candidate that violates [`B`].
    Switched,
    /// Reports a candidate with another delay parameter.
    Assumptions,
    /// Reports a candidate without the trigger.
    WitnessLost,
    /// Reports a candidate, then changes its `input.bin`.
    Tampered,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Minimized => "minimized",
            Mode::Kept => "kept",
            Mode::Abort => "abort",
            Mode::OtherInput => "other-input",
            Mode::OtherBuild => "other-build",
            Mode::Switched => "switched",
            Mode::Assumptions => "assumptions",
            Mode::WitnessLost => "witness-lost",
            Mode::Tampered => "tampered",
        }
    }

    fn parse(name: &str) -> Mode {
        [
            Mode::Minimized,
            Mode::Kept,
            Mode::Abort,
            Mode::OtherInput,
            Mode::OtherBuild,
            Mode::Switched,
            Mode::Assumptions,
            Mode::WitnessLost,
            Mode::Tampered,
        ]
        .into_iter()
        .find(|mode| mode.name() == name)
        .unwrap_or_else(|| panic!("unknown mode {name}"))
    }
}

/// The variable that makes [`triage_minimize_child`] record, replay or
/// minimize.
const CHILD: &str = "ZAKURA_SIM_TRIAGE_MINIMIZE_CHILD";

/// The variable that names the file the stand-in minimizer appends each
/// input path it gets to.
const LOG: &str = "ZAKURA_SIM_TRIAGE_MINIMIZE_LOG";

/// Records, replays or minimizes when run as a child, and does nothing
/// otherwise.
///
/// - `record:<input>:<dir>` records the input in the file `<input>`.
/// - `replay:<dir>` replays the trace in `<dir>`, and fails unless it
///   verified.
/// - `minimize:<mode>:<property>:<input>:<out>` is the stand-in
///   minimizer ([`stand_in`]).
#[test]
fn triage_minimize_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let parts: Vec<&str> = mode.splitn(5, ':').collect();
    match parts[..] {
        ["record", input, dir] => {
            let input = fs::read(input).unwrap();
            record(&PLANTED, RecordInput::Input(&input), Path::new(dir)).unwrap();
        }
        ["replay", dir] => {
            let trace = TraceReader::open(Path::new(dir)).unwrap();
            let report = replay_with(&trace, &PLANTED, ReplayOptions::default()).unwrap();
            assert!(report.verified(), "{report:?}");
        }
        ["minimize", mode, property, input, out] => {
            stand_in(
                Mode::parse(mode),
                property,
                Path::new(input),
                Path::new(out),
            );
        }
        _ => panic!("unknown {CHILD} mode {mode}"),
    }
}

/// The SHA-256 of `bytes`, in lowercase hex.
fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Writes `value` as pretty JSON to `path`.
fn write_json(path: &Path, value: &impl Serialize) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string_pretty(value).unwrap() + "\n").unwrap();
}

/// The JSON file at `path`.
fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

/// Confirms `input` against `property` into `<out>/<dir>/`, as `cargo xtask
/// sim-minimize` checks an input, and returns the check's record for its
/// report.
fn check_into(out: &Path, dir: &str, input: &[u8], property: &str) -> Value {
    let confirm_dir = out.join(dir).join("confirm");
    fs::create_dir_all(&confirm_dir).unwrap();
    fs::write(out.join(dir).join("input.bin"), input).unwrap();
    let confirmation = confirm(&PLANTED, input, property, &confirm_dir).unwrap();
    write_json(&confirm_dir.join(CONFIRMATION_FILE), &confirmation);
    let trace = confirmation
        .trace
        .as_ref()
        .map(|trace| format!("{dir}/confirm/{}", trace.display()));
    json!({
        "accepted": true,
        "reason": null,
        "confirmation": confirmation.status,
        "crash": "expected",
        "detail": null,
        "confirmation_file": format!("{dir}/confirm/{CONFIRMATION_FILE}"),
        "trace": trace,
    })
}

/// The stand-in minimizer: writes `cargo xtask sim-minimize`'s output for
/// `input` and `property` into `out`, as `mode` says, without `tmin`.
fn stand_in(mode: Mode, property: &str, input: &Path, out: &Path) {
    let log = std::env::var(LOG).unwrap();
    let mut line = fs::read_to_string(&log).unwrap_or_default();
    line.push_str(&format!("{}\n", input.display()));
    fs::write(&log, line).unwrap();
    if mode == Mode::Abort {
        std::process::abort();
    }
    fs::create_dir_all(out.join("bin")).unwrap();
    fs::write(out.join("bin").join("zakura-sim"), b"the pinned binary").unwrap();

    let mut original = fs::read(input).unwrap();
    if mode == Mode::OtherInput {
        original.push(0);
    }
    let original_check = check_into(out, "original", &original, property);
    if mode == Mode::OtherBuild {
        let path = out.join("original/confirm").join(CONFIRMATION_FILE);
        let mut confirmation = read_json(&path);
        confirmation["build"]["binary"] = json!("0".repeat(64));
        confirmation["build"]["dirty"] = json!(true);
        write_json(&path, &confirmation);
    }
    let mut report = json!({
        "format": 1,
        "status": "original-kept",
        "reason": "every candidate smaller than the original was rejected",
        "target": "stand_in",
        "harness": PLANTED.name,
        "property": property,
        "budget": { "runs": 1, "wall_ms": 1000, "child_ms": 1000 },
        "build": null,
        "original": {
            "path": "original/input.bin",
            "sha256": sha256(&original),
            "len": original.len(),
            "check": original_check,
        },
        "reduction": { "end": "finished", "completed": true, "detail": null, "elapsed_ms": 0, "log": null },
        "counts": { "produced": 0, "checked": 0, "accepted": 0, "rejected": 0 },
        "candidates": [],
        "minimized": null,
        "elapsed_ms": 0,
    });
    if mode == Mode::Kept {
        write_json(&out.join("report.json"), &report);
        std::process::exit(1);
    }

    let mut candidate = original[..3].to_vec();
    let mut candidate_property = property;
    match mode {
        Mode::Switched => {
            candidate[0] = FAULT_B;
            candidate_property = B;
        }
        Mode::Assumptions => candidate[1] += 1,
        Mode::WitnessLost => candidate[2] = 0,
        _ => {}
    }
    let check = check_into(out, "minimized", &candidate, candidate_property);
    if mode == Mode::Tampered {
        fs::write(out.join("minimized/input.bin"), [FAULT_A, 7, 3]).unwrap();
    }
    let digest = sha256(&candidate);
    report["status"] = json!("minimized");
    report["reason"] = Value::Null;
    report["counts"] = json!({ "produced": 1, "checked": 1, "accepted": 1, "rejected": 0 });
    report["candidates"] = json!([{
        "file": "tmin/minimized-from-0",
        "sha256": digest,
        "len": candidate.len(),
        "check": check,
    }]);
    report["minimized"] = json!({
        "dir": "minimized",
        "sha256": digest,
        "len": candidate.len(),
        "trace": check["trace"].as_str().unwrap_or("minimized/confirm/traces"),
    });
    write_json(&out.join("report.json"), &report);
}

/// This test binary as [`triage_minimize_child`], without core dumps, with
/// the stand-in minimizer in `mode`, or no minimizer. With
/// `replay_minimized_fails`, the replay of a minimized trace fails.
struct TestChildren {
    mode: Option<Mode>,
    log: PathBuf,
    replay_minimized_fails: bool,
}

impl TestChildren {
    fn command(&self, mode: String) -> Command {
        let mut command = Command::new("sh");
        command
            .args(["-c", "ulimit -c 0; exec \"$0\" \"$@\""])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "triage_minimize_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, mode)
            .env(LOG, &self.log);
        command
    }
}

impl Children for TestChildren {
    fn record(&self, input: &Path, out: &Path) -> Command {
        self.command(format!("record:{}:{}", input.display(), out.display()))
    }

    fn replay(&self, trace: &Path) -> Command {
        if self.replay_minimized_fails
            && trace
                .components()
                .any(|part| part.as_os_str() == MINIMIZER_DIR)
        {
            return Command::new("false");
        }
        self.command(format!("replay:{}", trace.display()))
    }

    fn minimize(&self, input: &Path, property: &str, out: &Path) -> Option<Command> {
        let mode = self.mode?;
        Some(self.command(format!(
            "minimize:{}:{property}:{}:{}",
            mode.name(),
            input.display(),
            out.display()
        )))
    }
}

/// A scratch directory with an archive under `archive/` and the stand-in's
/// log.
struct Scratch {
    dir: TempDir,
}

impl Scratch {
    fn new() -> Self {
        Scratch {
            dir: TempDir::new().unwrap(),
        }
    }

    fn out(&self) -> PathBuf {
        self.dir.path().join("archive")
    }

    fn log(&self) -> PathBuf {
        self.dir.path().join("minimizer.log")
    }

    /// Each input path the stand-in minimizer got, in order.
    fn minimizer_inputs(&self) -> Vec<PathBuf> {
        fs::read_to_string(self.log())
            .unwrap_or_default()
            .lines()
            .map(PathBuf::from)
            .collect()
    }

    fn children(&self, mode: Option<Mode>) -> TestChildren {
        TestChildren {
            mode,
            log: self.log(),
            replay_minimized_fails: false,
        }
    }

    /// Triages `input`, saved as `name`, with `children`.
    fn triage_with(&self, name: &str, input: &[u8], children: &TestChildren) -> Triaged {
        let artifact = self.dir.path().join(name);
        fs::write(&artifact, input).unwrap();
        let options = TriageOptions {
            child_timeout: Duration::from_secs(60),
        };
        triage(&PLANTED, children, &artifact, &self.out(), options).unwrap()
    }

    /// Triages `input` with the stand-in minimizer in `mode`, or none.
    fn triage(&self, name: &str, input: &[u8], mode: Option<Mode>) -> Triaged {
        self.triage_with(name, input, &self.children(mode))
    }
}

/// The minimized bundle `triaged` kept.
fn kept(triaged: &Triaged) -> (PathBuf, bool) {
    match &triaged.minimization {
        Some(Minimization::Kept { bundle, archived }) => (bundle.clone(), *archived),
        other => panic!("expected a minimized bundle, got {other:?}"),
    }
}

/// `bundle`'s name followed by `suffix`, beside it.
fn named(bundle: &Path, suffix: &str) -> PathBuf {
    let name = bundle.file_name().unwrap().to_str().unwrap();
    bundle.with_file_name(format!("{name}{suffix}"))
}

/// True if nothing is left under the archive's staging directory.
fn staging_is_empty(scratch: &Scratch) -> bool {
    fs::read_dir(scratch.out().join(STAGING_DIR))
        .unwrap()
        .next()
        .is_none()
}

/// A confirmed `Active` failure gets a minimized bundle beside its bundle.
/// The minimizer gets the placed bundle's input, so the failure's bundle is
/// in place first and holds none of the minimizer's files. The minimized
/// bundle holds the candidate, the minimizer's output without its pinned
/// binaries, a fresh replay, and the failure's bundle's checksums digest.
/// Both verify, and the binary's `verify-archive` counts both.
#[test]
fn a_confirmed_failure_gains_a_minimized_bundle() {
    let scratch = Scratch::new();
    let triaged = scratch.triage("crash", &ORIGINAL, Some(Mode::Minimized));
    assert_eq!(triaged.status, TriageStatus::Confirmed, "{triaged:?}");
    assert_eq!(triaged.bucket, Some(Bucket::Active));
    assert!(triaged.archived);
    let original = &triaged.bundle;
    let (minimized, archived) = kept(&triaged);
    assert!(archived);
    assert_eq!(minimized, named(original, ".minimized"));
    assert_eq!(
        scratch.minimizer_inputs(),
        [original.join(BUNDLE_INPUT_FILE)]
    );

    assert_eq!(verify(original).unwrap(), Vec::<String>::new());
    assert_eq!(verify(&minimized).unwrap(), Vec::<String>::new());
    let mut both = vec![original.clone(), minimized.clone()];
    both.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), both);
    let sums = fs::read(original.join(CHECKSUMS_FILE)).unwrap();
    assert!(!String::from_utf8_lossy(&sums).contains(MINIMIZER_DIR));
    assert_eq!(read_json(&original.join(TRIAGE_FILE))["format"], 1);

    assert_eq!(
        fs::read(minimized.join(BUNDLE_INPUT_FILE)).unwrap(),
        ORIGINAL[..3]
    );
    let report = read_json(&minimized.join(MINIMIZED_FILE));
    assert_eq!(report["format"], 1);
    assert_eq!(report["property"], A);
    assert_eq!(report["signature"], format!("active {A}"));
    assert_eq!(
        report["original"]["bundle"],
        original.file_name().unwrap().to_str().unwrap()
    );
    assert_eq!(report["original"]["checksums_sha256"], sha256(&sums));
    assert_eq!(report["original"]["input"]["sha256"], sha256(&ORIGINAL));
    assert_eq!(report["input"]["sha256"], sha256(&ORIGINAL[..3]));
    assert_eq!(report["replay"]["verified"], true);
    let copy = minimized.join(MINIMIZER_DIR);
    assert!(copy.join("report.json").is_file());
    assert!(copy
        .join("original/confirm")
        .join(CONFIRMATION_FILE)
        .is_file());
    assert!(
        !copy.join("bin").exists(),
        "the bundle keeps no pinned binary"
    );
    assert!(minimized
        .join(RUNS_DIR)
        .join("minimizer")
        .join("stdout.txt")
        .is_file());
    let trace = TraceReader::open(&minimized.join(report["trace"].as_str().unwrap())).unwrap();
    let replayed = replay_with(&trace, &PLANTED, ReplayOptions::default()).unwrap();
    assert!(replayed.verified());
    assert!(staging_is_empty(&scratch));

    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("verify-archive")
        .arg(scratch.out())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains(&format!(": active {A}, minimized\n")),
        "{stdout}"
    );
    assert!(
        stdout.ends_with("2 bundles match their checksums\n"),
        "{stdout}"
    );
}

/// A later triage with a minimizer adds a minimized bundle to a failure
/// archived without one, and keeps a minimized bundle that verifies
/// without running the minimizer again. Beside a minimized bundle that
/// fails its checksums, it minimizes again and places its own as
/// `.minimized-1`, and the failed one stays as it was.
#[test]
fn a_later_triage_adds_or_keeps_the_minimized_bundle() {
    let scratch = Scratch::new();
    let first = scratch.triage("crash", &ORIGINAL, None);
    assert_eq!(first.status, TriageStatus::Confirmed);
    assert_eq!(first.minimization, None);

    let added = scratch.triage("crash-again", &ORIGINAL, Some(Mode::Minimized));
    assert!(!added.archived, "{added:?}");
    assert_eq!(added.bundle, first.bundle);
    let (minimized, archived) = kept(&added);
    assert!(archived);
    assert_eq!(scratch.minimizer_inputs().len(), 1);

    let again = scratch.triage("crash-third", &ORIGINAL, Some(Mode::Minimized));
    assert_eq!(kept(&again), (minimized.clone(), false));
    assert_eq!(
        scratch.minimizer_inputs().len(),
        1,
        "the minimizer didn't run"
    );

    fs::write(minimized.join("extra.txt"), b"added").unwrap();
    let damaged = verify(&minimized).unwrap();
    assert_eq!(damaged, ["extra.txt isn't in SHA256SUMS"]);
    let replaced = scratch.triage("crash-fourth", &ORIGINAL, Some(Mode::Minimized));
    assert_eq!(
        kept(&replaced),
        (named(&first.bundle, ".minimized-1"), true)
    );
    assert_eq!(scratch.minimizer_inputs().len(), 2);
    assert_eq!(verify(&minimized).unwrap(), damaged);
    assert!(staging_is_empty(&scratch));
}

/// The minimizer's output is kept only if its report has an accepted
/// candidate on the bundle's harness, property and input, both
/// confirmations hold to the bundle, the candidate's holds to the
/// original's, its input is the one confirmed, and a fresh replay matches
/// its trace. Otherwise triage keeps the failure's bundle alone, and the
/// minimizer's output stays under staging.
#[test]
fn minimizer_output_that_doesnt_hold_is_not_kept() {
    let rows = [
        (
            Mode::Kept,
            false,
            "exited with code 1; its report says original-kept",
        ),
        (Mode::Abort, false, "the minimizer died from signal 6"),
        (Mode::OtherInput, false, "the minimizer ran input"),
        (
            Mode::OtherBuild,
            false,
            "the original input's confirmation ran on a build",
        ),
        (
            Mode::Switched,
            false,
            "the minimized input's confirmation confirms test/triage-minimize/b",
        ),
        (
            Mode::Assumptions,
            false,
            "held to the original's confirmation, the minimized input is assumptions-changed",
        ),
        (
            Mode::WitnessLost,
            false,
            "the minimized input's confirmation is witness-lost",
        ),
        (Mode::Tampered, false, "not the minimized input's"),
        (
            Mode::Minimized,
            true,
            "a fresh replay of the minimized trace exited with code 1",
        ),
    ];
    for (mode, replay_minimized_fails, expected) in rows {
        let scratch = Scratch::new();
        let children = TestChildren {
            replay_minimized_fails,
            ..scratch.children(Some(mode))
        };
        let triaged = scratch.triage_with("crash", &ORIGINAL, &children);
        assert_eq!(triaged.status, TriageStatus::Confirmed, "{mode:?}");
        let Some(Minimization::NotKept { reason, staging }) = &triaged.minimization else {
            panic!("{mode:?}: {:?}", triaged.minimization);
        };
        assert!(reason.contains(expected), "{mode:?}: {reason}");
        assert!(staging.starts_with(scratch.out().join(STAGING_DIR)));
        assert!(
            staging.join("output").is_dir() || mode == Mode::Abort,
            "{mode:?}"
        );
        assert!(
            !staging.join("output").join("bin").exists(),
            "{mode:?}: the staging directory keeps no pinned binary"
        );
        assert_eq!(
            bundles(&scratch.out()).unwrap(),
            std::slice::from_ref(&triaged.bundle),
            "{mode:?}"
        );
        assert_eq!(verify(&triaged.bundle).unwrap(), Vec::<String>::new());
    }
}

/// Only a confirmed `Active` failure goes to the minimizer: not a clean
/// run, and not a confirmed harness failure.
#[test]
fn only_confirmed_active_failures_are_minimized() {
    let scratch = Scratch::new();
    let clean = scratch.triage("clean", &[FAULT_NONE, 7, 1], Some(Mode::Minimized));
    assert_eq!(clean.status, TriageStatus::NotReproduced);
    assert_eq!(clean.minimization, None);
    let harness = scratch.triage("harness", &[FAULT_HARNESS, 7, 1], Some(Mode::Minimized));
    assert_eq!(harness.status, TriageStatus::Confirmed);
    assert_eq!(harness.bucket, Some(Bucket::Harness));
    assert_eq!(harness.minimization, None);
    assert!(scratch.minimizer_inputs().is_empty());
}

/// Rewrites `dir`'s `SHA256SUMS` to match its files, as an operator who
/// re-hashes a changed bundle would.
fn rehash(dir: &Path) {
    let mut lines = Vec::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        for item in fs::read_dir(dir.join(&relative)).unwrap() {
            let item = item.unwrap();
            let path = relative.join(item.file_name());
            if item.file_type().unwrap().is_dir() {
                pending.push(path);
            } else if path != Path::new(CHECKSUMS_FILE) {
                let bytes = fs::read(dir.join(&path)).unwrap();
                lines.push(format!("{}  {}\n", sha256(&bytes), path.display()));
            }
        }
    }
    lines.sort_by(|a, b| a[66..].cmp(&b[66..]));
    fs::write(dir.join(CHECKSUMS_FILE), lines.concat()).unwrap();
}

/// Copies the directory `from` to `to`.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for item in fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let target = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_dir(&item.path(), &target);
        } else {
            fs::copy(item.path(), target).unwrap();
        }
    }
}

/// `verify` holds a minimized bundle to its failure's bundle, beyond its
/// checksums: a copy beside another failure's bundle, a re-hashed bundle
/// whose `minimized.json` names another input, a failure's bundle whose
/// checksums changed or that's gone, and a minimized bundle that lost
/// `minimized.json` all fail, and `bundles` finds each.
#[test]
fn verify_holds_a_minimized_bundle_to_its_failure() {
    let scratch = Scratch::new();
    let triaged = scratch.triage("crash", &ORIGINAL, Some(Mode::Minimized));
    let (minimized, _) = kept(&triaged);
    let other = scratch.triage("other", &[FAULT_A, 7, 1, 8], None).bundle;
    let other_name = other.file_name().unwrap().to_str().unwrap().to_owned();
    let name = triaged
        .bundle
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let stale = named(&other, ".minimized");
    copy_dir(&minimized, &stale);
    assert_eq!(verify_bundle(&stale).unwrap(), Vec::<String>::new());
    assert_eq!(
        verify(&stale).unwrap(),
        [format!(
            "{other_name}.minimized isn't named after the failure's bundle {name}"
        )]
    );

    let renamed = named(&triaged.bundle, ".minimized-1");
    copy_dir(&minimized, &renamed);
    let mut report = read_json(&renamed.join(MINIMIZED_FILE));
    report["input"]["sha256"] = json!(sha256(b"another input"));
    write_json(&renamed.join(MINIMIZED_FILE), &report);
    rehash(&renamed);
    assert_eq!(verify_bundle(&renamed).unwrap(), Vec::<String>::new());
    let problems = verify(&renamed).unwrap();
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(
        problems[0].starts_with("minimized.json names input"),
        "{problems:?}"
    );

    let lost = named(&triaged.bundle, ".minimized-2");
    copy_dir(&minimized, &lost);
    fs::remove_file(lost.join(MINIMIZED_FILE)).unwrap();
    assert_eq!(
        verify(&lost).unwrap(),
        [format!("{MINIMIZED_FILE} is missing")]
    );

    // `verify-archive` prints the signature from `minimized.json`, so a
    // re-hashed edit to it fails too, as does one to the trace it names.
    let edited = named(&triaged.bundle, ".minimized-3");
    copy_dir(&minimized, &edited);
    let mut report = read_json(&edited.join(MINIMIZED_FILE));
    report["signature"] = json!(format!("active {B}"));
    report["trace"] = json!(format!("{MINIMIZER_DIR}/original/confirm/traces"));
    write_json(&edited.join(MINIMIZED_FILE), &report);
    rehash(&edited);
    assert_eq!(verify_bundle(&edited).unwrap(), Vec::<String>::new());
    let problems = verify(&edited).unwrap();
    assert_eq!(problems.len(), 2, "{problems:?}");
    assert!(
        problems[0].starts_with("minimized.json names signature \"active test/triage-minimize/b\""),
        "{problems:?}"
    );
    assert!(
        problems[1].starts_with("minimized.json names trace"),
        "{problems:?}"
    );

    let mut all = vec![
        triaged.bundle.clone(),
        minimized.clone(),
        other.clone(),
        stale.clone(),
        renamed.clone(),
        lost.clone(),
        edited.clone(),
    ];
    all.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), all);

    let sums = triaged.bundle.join(CHECKSUMS_FILE);
    let mut text = fs::read(&sums).unwrap();
    text.extend_from_slice(b"\n");
    fs::write(&sums, text).unwrap();
    assert_eq!(
        verify(&minimized).unwrap(),
        [format!(
            "the failure's bundle {name} has another SHA256SUMS than the one it was minimized from"
        )]
    );
    fs::remove_dir_all(&triaged.bundle).unwrap();
    let problems = verify(&minimized).unwrap();
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(
        problems[0].starts_with(&format!(
            "the failure's bundle {name} has no SHA256SUMS that reads"
        )),
        "{problems:?}"
    );

    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("verify-archive")
        .arg(scratch.out())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(", minimized\n"), "{stdout}");
}

/// `SimChildren` minimizes with `cargo xtask sim-minimize` in its
/// workspace, with absolute paths, every budget, and without the
/// simulator build's `RUSTFLAGS` and target directory. Without a
/// minimizer, it gives none.
#[test]
fn sim_children_run_sim_minimize() {
    let mut children = SimChildren {
        exe: PathBuf::from("zakura-sim"),
        harness: "fixtures/all".to_owned(),
        minimize: None,
    };
    let (input, out) = (Path::new("crash"), Path::new("archive/staging"));
    assert!(children.minimize(input, "race/lost-update", out).is_none());

    children.minimize = Some(SimMinimize {
        workspace: PathBuf::from("/repo"),
        target: "race_fixtures".to_owned(),
        runs: 64,
        wall: Duration::from_secs(300),
        child: Duration::from_secs(30),
        sanitizer: Some("none".to_owned()),
    });
    let command = children
        .minimize(input, "race/lost-update", out)
        .expect("a minimizer is set");
    let cwd = std::env::current_dir().unwrap();
    let args: Vec<String> = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        args,
        [
            "xtask".to_owned(),
            "sim-minimize".to_owned(),
            "race_fixtures".to_owned(),
            cwd.join("crash").display().to_string(),
            "--property".to_owned(),
            "race/lost-update".to_owned(),
            "--out".to_owned(),
            cwd.join("archive/staging").display().to_string(),
            "--runs".to_owned(),
            "64".to_owned(),
            "--wall-secs".to_owned(),
            "300".to_owned(),
            "--child-secs".to_owned(),
            "30".to_owned(),
            "--sanitizer".to_owned(),
            "none".to_owned(),
        ]
    );
    assert_eq!(command.get_current_dir(), Some(Path::new("/repo")));
    let removed: Vec<_> = command
        .get_envs()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| name.to_string_lossy().into_owned())
        .collect();
    assert_eq!(removed, ["CARGO_TARGET_DIR", "RUSTFLAGS"]);
}

/// The binary's `triage` takes `--target`, `--runs` and `--wall-secs`
/// together, and `--sanitizer` only with them.
#[test]
fn the_binary_takes_the_minimizer_flags_together() {
    let scratch = Scratch::new();
    let input = scratch.dir.path().join("input");
    fs::write(&input, [0u8]).unwrap();
    let out = scratch.out();
    let base = [
        "triage",
        "--sim",
        "fixtures/all",
        "--out",
        out.to_str().unwrap(),
    ];
    for flags in [
        &["--target", "race_fixtures"][..],
        &["--runs", "3", "--wall-secs", "10"],
        &["--sanitizer", "none"],
        &[
            "--target",
            "race_fixtures",
            "--runs",
            "3",
            "--wall-secs",
            "10",
            "--sanitizer",
            "thread",
        ],
        &[
            "--target",
            "race_fixtures",
            "--runs",
            "0",
            "--wall-secs",
            "10",
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(base)
            .args(flags)
            .arg(&input)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{flags:?}: {output:?}");
    }
    assert!(!out.exists(), "a usage error archives nothing");
}

/// A `fixtures/all` input that loses an update in `race/lost-update`'s
/// planted variant, as in `minimize.rs`.
const LOST_UPDATE: [u8; 27] = [
    0x00, 0x00, 0x01, 0x5a, 0x46, 0x17, 0xb5, 0x61, 0x56, 0xad, 0x3e, 0x87, 0x76, 0x2d, 0xd4, 0x33,
    0x7e, 0xe2, 0xc5, 0x71, 0x8b, 0x3c, 0xc5, 0xc0, 0x7b, 0x6b, 0xff,
];

/// The binary's `triage` runs every input's children on a copy of itself
/// that it makes before the first input. A minimizer that replaces the
/// binary, as `cargo xtask sim-minimize`'s rebuild does, then can't change
/// what a later input's children run, so a second copy of the input files
/// where the first did. The copy is gone once `triage` exits.
#[test]
fn the_binary_pins_itself_for_the_batch() {
    let scratch = Scratch::new();
    let dir = scratch.dir.path();
    let exe = dir.join("zakura-sim");
    fs::copy(env!("CARGO_BIN_EXE_zakura-sim"), &exe).unwrap();
    // Stands in for `cargo`: replaces the binary with one that exits 3, by
    // rename, as cargo's build does, and fails as a minimizer.
    let cargo = dir.join("cargo");
    fs::write(
        &cargo,
        "#!/bin/sh\n\
         printf '#!/bin/sh\\nexit 3\\n' > \"$REBUILT.new\"\n\
         chmod +x \"$REBUILT.new\"\n\
         mv \"$REBUILT.new\" \"$REBUILT\"\n\
         exit 1\n",
    )
    .unwrap();
    fs::set_permissions(&cargo, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let inputs = ["first", "second"].map(|name| {
        let input = dir.join(name);
        fs::write(&input, LOST_UPDATE).unwrap();
        input
    });
    let out = scratch.out();
    let output = Command::new(&exe)
        .args(["triage", "--sim", "fixtures/all", "--out"])
        .arg(&out)
        .args([
            "--target",
            "race_fixtures",
            "--runs",
            "1",
            "--wall-secs",
            "1",
        ])
        .args(&inputs)
        .env("CARGO", &cargo)
        .env("REBUILT", &exe)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::read(&exe).unwrap(), b"#!/bin/sh\nexit 3\n");

    let [bundle] = <[PathBuf; 1]>::try_from(bundles(&out).unwrap()).unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let filed: Vec<&str> = stdout
        .lines()
        .filter(|line| !line.contains(": not minimized: "))
        .collect();
    let class = "confirmed in active: active race/lost-update";
    assert_eq!(
        filed,
        [
            format!(
                "{}: {class}; archived in {}",
                inputs[0].display(),
                bundle.display()
            ),
            format!(
                "{}: {class}; already archived in {}",
                inputs[1].display(),
                bundle.display()
            ),
        ],
        "{stdout}"
    );
    for staging in fs::read_dir(out.join(STAGING_DIR)).unwrap() {
        let staging = staging.unwrap().path();
        assert!(
            !staging.join("zakura-sim").exists(),
            "{}",
            staging.display()
        );
    }
}
