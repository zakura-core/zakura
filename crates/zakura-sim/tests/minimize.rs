//! T2b: `confirm` accepts a `cargo fuzz tmin` candidate only if it keeps
//! the original's property, assumptions, witness and evidence (traces
//! implementation plan, T2b; M7.4).
//!
//! The harness here plants two properties and a task panic, each picked by
//! a scenario byte, so a byte reduction can switch from one failure to
//! another, as `tmin` may.
#![cfg(zakura_sim)]

use std::{cell::Cell, path::Path, process::Command, rc::Rc};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use zakura_scheduler::{SchedulerConfig, Status as SchedulerStatus};
use zakura_sim::{
    harness::{entry, HarnessEntry, Subsystem, NO_TASK_PANIC},
    property::{
        Assume, AssumeRecord, Assumptions, CheckCtx, HasView, Kind, Property, PropertyId, Source,
        Sources, Status, Verdict,
    },
    trace::{confirm, ConfirmStatus, Confirmation, TraceReader, CONFIRMATION_FILE},
    Observation, ObservationLog, RunConfig, Sim,
};

/// The first planted property.
const A: &str = "test/minimize/a";

/// The second planted property.
const B: &str = "test/minimize/b";

/// What the run's `fault` task does, from scenario byte 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Fault {
    /// Breaks [`A`].
    A,
    /// Breaks [`B`].
    B,
    /// Panics, which violates `sim/no-task-panic`.
    Panic,
    /// Breaks nothing.
    None,
    /// Breaks [`A`] and [`B`].
    Both,
    /// Breaks [`A`], beside a task whose name the run refuses, so the run
    /// ends at a harness failure.
    HarnessFailure,
}

impl Fault {
    /// Every fault, in draw order.
    const ALL: [Fault; 6] = [
        Fault::A,
        Fault::B,
        Fault::Panic,
        Fault::None,
        Fault::Both,
        Fault::HarnessFailure,
    ];
}

/// The planted harness's scenario.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    /// What the `fault` task does.
    fault: Fault,
    /// The declared delivery bound's parameter. No property assumes it, so
    /// it changes only the declared assumptions.
    max_delay_ms: u8,
    /// Whether a `trigger` task runs first: the planted properties' trigger
    /// history.
    trigger: bool,
}

/// Two planted properties and a planted task panic.
struct Planted {
    /// Whether [`A`] and [`B`] are broken.
    broken: Rc<(Cell<bool>, Cell<bool>)>,
}

impl HasView for Planted {
    /// Whether [`A`] and [`B`] are broken.
    type View = (bool, bool);

    fn view(&self) -> (bool, bool) {
        (self.broken.0.get(), self.broken.1.get())
    }
}

impl Subsystem for Planted {
    const NAME: &'static str = "test/minimize";
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
        if scenario.fault == Fault::HarnessFailure {
            sim.scheduler().spawn_root("bad:name", async {});
        }
        let fault = scenario.fault;
        let state = Rc::clone(&broken);
        sim.scheduler().spawn_root("fault", async move {
            match fault {
                Fault::A | Fault::HarnessFailure => state.0.set(true),
                Fault::B => state.1.set(true),
                Fault::Both => {
                    state.0.set(true);
                    state.1.set(true);
                }
                Fault::Panic => panic!("planted task panic"),
                Fault::None => {}
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
            params: serde_json::json!({ "max_delay_ms": scenario.max_delay_ms }),
        }])
    }

    /// Byte 0 picks the fault, byte 1 is the delay parameter, and byte 2's
    /// low bit runs `trigger`.
    fn draw(prefix: &[u8]) -> Scenario {
        Scenario {
            fault: Fault::ALL[usize::from(prefix[0]) % Fault::ALL.len()],
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

/// True if [`A`] is broken.
fn broke_a((a, _): (bool, bool)) -> bool {
    a
}

/// True if [`B`] is broken.
fn broke_b((_, b): (bool, bool)) -> bool {
    b
}

/// [`A`]'s check.
fn check_a(cx: &mut CheckCtx<'_, Planted>) -> Verdict {
    check(cx, broke_a)
}

/// [`B`]'s check.
fn check_b(cx: &mut CheckCtx<'_, Planted>) -> Verdict {
    check(cx, broke_b)
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

/// The original input: fault [`A`], a 7 ms delay bound, the trigger, and
/// Plan bytes a reduction may drop.
const ORIGINAL: [u8; 6] = [0, 7, 1, 9, 9, 9];

/// Confirms `input` against `property` in a new directory.
fn run(input: &[u8], property: &str) -> (Confirmation, TempDir) {
    let dir = TempDir::new().unwrap();
    let confirmation = confirm(&PLANTED, input, property, dir.path()).unwrap();
    (confirmation, dir)
}

/// The confirmed original, and its directory.
fn original() -> (Confirmation, TempDir) {
    let (original, dir) = run(&ORIGINAL, A);
    assert_eq!(original.status, ConfirmStatus::Confirmed, "{original:?}");
    (original, dir)
}

/// `candidate` confirmed against `property` and held to `original`.
fn candidate(original: &Confirmation, candidate: &[u8], property: &str) -> Confirmation {
    run(candidate, property).0.against(original)
}

/// M7.4: a reduction that keeps the property is accepted with a trace that
/// replays, and one that switches to the other planted property or to the
/// task panic isn't.
#[test]
fn tmin_keeps_the_property() {
    let (original, dir) = original();
    let trace = TraceReader::open(&dir.path().join(original.trace.as_ref().unwrap())).unwrap();
    assert_eq!(Some(&trace.id), original.trace_id.as_ref());
    assert_eq!(original.selected.as_deref(), Some(A));
    assert_eq!(original.input.len, ORIGINAL.len() as u64);

    let kept = candidate(&original, &ORIGINAL[..3], A);
    assert_eq!(kept.status, ConfirmStatus::Confirmed, "{kept:?}");
    assert!(kept.trace.is_some());

    for (input, other) in [([1, 7, 1], B), ([2, 7, 1], NO_TASK_PANIC)] {
        let switched = candidate(&original, &input, A);
        assert_eq!(
            switched.status,
            ConfirmStatus::OtherProperty,
            "{switched:?}"
        );
        assert_eq!(switched.selected.as_deref(), Some(other));
        assert_eq!(
            switched.detail.as_deref(),
            Some(format!("the run fails on {other}").as_str())
        );
        assert_eq!(switched.trace, None);
    }
}

/// A reduction of the delay byte changes the declared assumption's
/// parameter, so it doesn't reproduce the original's contract.
#[test]
fn changed_assumption_parameters_are_rejected() {
    let (original, _dir) = original();
    let changed = candidate(&original, &[0, 0, 1], A);
    assert_eq!(
        changed.status,
        ConfirmStatus::AssumptionsChanged,
        "{changed:?}"
    );
    assert_eq!(
        changed.assumptions[0].params,
        serde_json::json!({ "max_delay_ms": 0 })
    );
}

/// A reduction that drops the trigger task still violates the property,
/// but without its trigger history.
#[test]
fn a_missing_witness_is_rejected() {
    let (original, _dir) = original();
    let lost = candidate(&original, &[0, 7, 0], A);
    assert_eq!(lost.status, ConfirmStatus::WitnessLost, "{lost:?}");
}

/// A run that ends at a harness failure has incomplete evidence, whatever
/// it violated before.
#[test]
fn incomplete_evidence_is_rejected() {
    let (original, _dir) = original();
    let incomplete = candidate(&original, &[5, 7, 1], A);
    assert_eq!(
        incomplete.status,
        ConfirmStatus::IncompleteEvidence,
        "{incomplete:?}"
    );
    assert_eq!(incomplete.outcome, "harness-failure");
}

/// An input that violates both planted properties keeps both reports, and
/// the filter can't select either alone.
#[test]
fn several_failures_keep_every_report() {
    let (both, _dir) = run(&[4, 7, 1], A);
    assert_eq!(both.status, ConfirmStatus::SelectionUnsupported, "{both:?}");
    let violated: Vec<_> = both.violated.iter().map(|v| v.property.as_str()).collect();
    assert_eq!(violated, [A, B]);
}

/// `sim/no-task-panic` has no trigger-history check, so no input can show
/// its causal witness, and one that violates nothing is no failure.
#[test]
fn unsupported_and_clean_runs_say_so() {
    let (panic, _dir) = run(&[2, 7, 1], NO_TASK_PANIC);
    assert_eq!(panic.status, ConfirmStatus::WitnessUnsupported, "{panic:?}");
    let (clean, _dir) = run(&[3, 7, 1], A);
    assert_eq!(clean.status, ConfirmStatus::NotViolated, "{clean:?}");
    assert_eq!(clean.detail.as_deref(), Some("the run is clean"));
}

/// A candidate from another generator or build doesn't reproduce the
/// original, and a confirmation reads back as it was written, as `zakura-sim
/// confirm --against` reads it.
#[test]
fn provenance_changes_are_rejected_and_confirmations_round_trip() {
    let (original, _dir) = original();
    let text = serde_json::to_string(&original).unwrap();
    assert_eq!(
        serde_json::from_str::<Confirmation>(&text).unwrap(),
        original
    );

    let (kept, _dir) = run(&ORIGINAL[..3], A);
    let mut older = original.clone();
    older.generator += 1;
    let changed = kept.clone().against(&older);
    assert_eq!(
        changed.status,
        ConfirmStatus::ProvenanceChanged,
        "{changed:?}"
    );

    let mut other_build = original.clone();
    other_build.build.binary = "0".repeat(64);
    other_build.build.dirty = true;
    let changed = kept.against(&other_build);
    assert_eq!(
        changed.status,
        ConfirmStatus::ProvenanceChanged,
        "{changed:?}"
    );
}

/// A `fixtures/all` input that loses an update in `race/lost-update`'s
/// planted variant with two workers: the scenario prefix `[0, 0, 1]`, then
/// Plan bytes.
const LOST_UPDATE: [u8; 27] = [
    0x00, 0x00, 0x01, 0x5a, 0x46, 0x17, 0xb5, 0x61, 0x56, 0xad, 0x3e, 0x87, 0x76, 0x2d, 0xd4, 0x33,
    0x7e, 0xe2, 0xc5, 0x71, 0x8b, 0x3c, 0xc5, 0xc0, 0x7b, 0x6b, 0xff,
];

/// Runs the `zakura-sim` binary with `args`, and returns its exit code.
fn zakura_sim(args: &[&Path]) -> i32 {
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs");
    output.status.code().expect("the binary exits")
}

/// `zakura-sim confirm` confirms in a fresh process, writes the
/// confirmation, and records a trace that a second fresh process replays
/// Exact. It exits 1 with the finding for another property, and 2 for a
/// property the harness doesn't register.
#[test]
fn the_binary_confirms_in_a_fresh_process() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input");
    std::fs::write(&input, LOST_UPDATE).unwrap();
    let confirm = |property: &str, out: &str, against: Option<&Path>| {
        let out = dir.path().join(out);
        let mut args = vec![
            Path::new("confirm"),
            Path::new("--sim"),
            Path::new("fixtures/all"),
            Path::new("--input"),
            &input,
            Path::new("--property"),
            Path::new(property),
            Path::new("--out"),
            &out,
        ];
        if let Some(against) = against {
            args.extend([Path::new("--against"), against]);
        }
        let code = zakura_sim(&args);
        let confirmation = std::fs::read(out.join(CONFIRMATION_FILE))
            .ok()
            .map(|bytes| serde_json::from_slice::<Confirmation>(&bytes).unwrap());
        (code, confirmation, out)
    };

    let (code, original, out) = confirm("race/lost-update", "original", None);
    let original = original.unwrap();
    assert_eq!((code, original.status), (0, ConfirmStatus::Confirmed));
    let trace = out.join(original.trace.as_ref().unwrap());
    assert_eq!(zakura_sim(&[Path::new("replay"), &trace]), 0);

    let file = out.join(CONFIRMATION_FILE);
    let (code, again, _) = confirm("race/lost-update", "again", Some(&file));
    assert_eq!((code, again.unwrap().status), (0, ConfirmStatus::Confirmed));

    let (code, other, _) = confirm("race/ab-ba", "other", None);
    let other = other.unwrap();
    assert_eq!((code, other.status), (1, ConfirmStatus::OtherProperty));
    assert_eq!(other.selected.as_deref(), Some("race/lost-update"));

    let (code, none, _) = confirm("test/minimize/a", "unregistered", None);
    assert_eq!((code, none), (2, None));
}

/// `zakura-sim properties` prints the harness's registry, one ID per line,
/// which `cargo xtask sim-minimize` checks `--property` against before it
/// runs the fuzz target. It exits 2 for an unknown harness.
#[test]
fn the_binary_lists_a_harness_properties() {
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(["properties", "--sim", "fixtures/all"])
        .output()
        .expect("the binary runs");
    assert!(output.status.success(), "{output:?}");
    let listed: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let entry = zakura_sim::harness::find("fixtures/all").unwrap();
    let registered: Vec<String> = (entry.properties)()
        .into_iter()
        .map(|property| property.id)
        .collect();
    assert_eq!(listed, registered);
    assert!(listed.iter().any(|id| id == "race/lost-update"));
    assert!(!listed.iter().any(|id| id == A));

    assert_eq!(
        zakura_sim(&[
            Path::new("properties"),
            Path::new("--sim"),
            Path::new("fixtures/none")
        ]),
        2
    );
}
