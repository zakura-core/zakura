//! sched-4 S4.4 and S4.5: each race fixture's saved violating schedule.
//!
//! `tests/fixtures/race-schedules/` holds one seed per fixture: the planted
//! race on 2 workers, then Plan bytes that reach the race's trigger
//! history. They sit outside `tests/fixtures/` itself, where the seed
//! corpus lives, because they fail by design: a fuzz campaign seeded with
//! one would find its race at once.
//!
//! - S4.4: each saved schedule reproduces its race's violation with the
//!   race's trigger history, and the fix holds on the same Plan bytes. This
//!   file checks the seeds and runs their violation witnesses through the
//!   registered harnesses. The witness tests in `race_fixtures.rs` run the
//!   same Plan bytes on the race and on the fix.
//! - S4.5: a fresh process replays each one's trace exactly and records the
//!   same trace from the saved bytes, and Exact replay on changed code
//!   reports a divergence.
#![cfg(zakura_sim)]

use std::{
    collections::BTreeMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use tempfile::TempDir;
use zakura_scheduler::Status;
use zakura_sim::{
    harness::{
        self,
        fixtures::{
            AbBa, AbortWithoutJoin, Fixture, LostUpdate, MissedNotify, Scenario, SnapshotReserve,
            TimerCancel, Variant, Which,
        },
        record, record_witness, HarnessEntry, RecordInput, Subsystem,
    },
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, TriggerHistory},
    property::{Status as PropertyStatus, Verdict},
    seed::{self, Seed},
    trace::{replay, replay_with, ReplayError, ReplayOptions, Trace, TraceReader},
    Divergence, RunConfig, Sim,
};

/// The saved schedules' directory.
fn schedules() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/race-schedules")
}

/// Every saved schedule, by its fixture's property. Each must validate as a
/// required seed of its fixture's own harness, at that harness's current
/// generator, and decode to the planted race on 2 workers.
fn saved() -> BTreeMap<&'static str, Seed> {
    let report = seed::load(&[schedules()], None).expect("the schedule directory lists");
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    assert!(report.quarantined.is_empty(), "{:?}", report.quarantined);
    let race = serde_json::to_value(Scenario {
        variant: Variant::Base,
        workers: 2,
    })
    .unwrap();
    let mut saved = BTreeMap::new();
    for seed in report.admitted {
        let fixture = Which::ALL
            .into_iter()
            .find(|which| which.property() == seed.sidecar.harness)
            .unwrap_or_else(|| panic!("{}: not a race fixture", seed.path.display()));
        assert_eq!(
            seed.path.file_stem().and_then(|stem| stem.to_str()),
            fixture.property().strip_prefix("race/"),
            "`race_fixtures.rs` finds each schedule by its fixture's name"
        );
        assert!(seed.sidecar.required, "{}", seed.path.display());
        assert_eq!(
            seed.sidecar.scenario_sha256,
            seed::scenario_sha256(&race),
            "{}: the planted race on 2 workers",
            seed.path.display()
        );
        let witnesses: Vec<(&str, &str)> = seed
            .sidecar
            .witness
            .iter()
            .map(|w| (w.property.as_str(), w.expect.as_str()))
            .collect();
        assert_eq!(witnesses, [(fixture.property(), "violated")]);
        assert!(saved.insert(fixture.property(), seed).is_none());
    }
    assert_eq!(saved.len(), Which::ALL.len(), "one schedule per fixture");
    saved
}

/// S4.4: one saved schedule per race fixture, each a valid seed of the
/// fixture's own harness that decodes to the planted race on 2 workers, and
/// whose witness, run through the registered harness, sees the violation.
/// `race_fixtures.rs`'s witness tests also run each one's Plan bytes on the
/// fix, which holds.
#[test]
fn each_race_has_a_saved_schedule() {
    for (name, seed) in saved() {
        let witness = &seed.sidecar.witness[0];
        let dir = TempDir::new().unwrap();
        let (_, result) = record_witness(
            harness::find(name).unwrap(),
            RecordInput::Input(&seed.input),
            dir.path(),
            &witness.property,
            witness.expect.parse().unwrap(),
            &[],
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        result.unwrap_or_else(|error| panic!("{name}: {error}"));
    }
}

/// The variable that names the parent's trace root to
/// [`race_schedule_child`].
const CHILD: &str = "ZAKURA_SIM_RACE_SCHEDULE_CHILD";

/// What the child prints once every check passed.
const VERIFIED: &str = "zakura-sim-race-schedules-verified";

/// Records every saved schedule under `root`, one directory per fixture.
fn record_all(root: &Path) -> BTreeMap<&'static str, Trace> {
    saved()
        .into_iter()
        .map(|(name, seed)| {
            let dir = root.join(name.replace('/', "-"));
            std::fs::create_dir(&dir).unwrap();
            let trace = record(
                harness::find(name).unwrap(),
                RecordInput::Input(&seed.input),
                &dir,
            )
            .unwrap();
            (name, trace)
        })
        .collect()
}

/// When run as the child, checks each trace under the root [`CHILD`] names
/// against this process's own run: an Exact replay of the parent's trace
/// verifies, and a recording of the committed bytes gives the same `init`,
/// steps and `end`. Does nothing otherwise.
#[test]
fn race_schedule_child() {
    let Some(root) = std::env::var_os(CHILD) else {
        return;
    };
    let own = TempDir::new().unwrap();
    for (name, recorded) in record_all(own.path()) {
        let parent = TraceReader::open(&Path::new(&root).join(name.replace('/', "-"))).unwrap();
        let report = replay(&parent, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(report.verified(), "{name}: {report:?}");
        assert_eq!(report.end, parent.end, "{name}");
        assert_eq!(
            (&recorded.init, &recorded.steps, &recorded.end),
            (&parent.init, &parent.steps, &parent.end),
            "{name}"
        );
    }
    #[allow(
        clippy::print_stdout,
        reason = "the parent test reads the child's stdout"
    )]
    {
        println!("\n{VERIFIED}");
    }
}

/// S4.5: two fresh processes each replay every saved schedule's trace with
/// the same decisions and violation, and record the same trace from the
/// committed bytes.
#[test]
fn fresh_processes_reproduce_each_saved_violation() {
    let root = TempDir::new().unwrap();
    for (name, trace) in record_all(root.path()) {
        let violation = trace.end.unwrap().violation;
        assert_eq!(violation.map(|v| v.property).as_deref(), Some(name));
    }
    for child in 0..2 {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "race_schedule_child", "--nocapture"])
            .env(CHILD, root.path())
            .output()
            .expect("the test binary reruns itself");
        assert!(output.status.success(), "child {child}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(VERIFIED),
            "child {child}: {output:?}"
        );
    }
}

/// A code change applied to a fixture.
trait Change: 'static {
    /// The fixture variant that carries the change.
    const VARIANT: Variant;
}

/// The fix.
struct ToFixed;

impl Change for ToFixed {
    const VARIANT: Variant = Variant::Fixed;
}

/// An unrelated task spawned before the fixture's tasks.
struct AddTask;

impl Change for AddTask {
    const VARIANT: Variant = Variant::UnrelatedTask;
}

/// An extra point of a kind the racing task doesn't otherwise reach.
struct AddNewKind;

impl Change for AddNewKind {
    const VARIANT: Variant = Variant::ExtraPointNewKind;
}

/// An extra point of a kind the racing task already reaches.
struct AddExistingKind;

impl Change for AddExistingKind {
    const VARIANT: Variant = Variant::ExtraPointExistingKind;
}

/// `race/<name>` with change `C` in its code: the same name, scenario,
/// config and property, but the fixture always runs `C::VARIANT`, whatever
/// the scenario says. A trace of the planted race replays on it as on a
/// build with that change.
struct Changed<F, C>(F, PhantomData<C>);

impl<F: Fixture, C: Change> HasView for Changed<F, C> {
    type View = F::View;

    /// The fixture's shared state.
    fn view(&self) -> F::View {
        self.0.view()
    }
}

impl<F: Fixture, C: Change> Subsystem for Changed<F, C> {
    const NAME: &'static str = F::NAME;
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 2;
    const PLANTED: bool = true;
    type Scenario = Scenario;
    type Prepared = ();

    /// The unchanged harness's config for `scenario`.
    fn config(scenario: &Scenario) -> RunConfig {
        let value = serde_json::to_value(scenario).unwrap();
        (harness::find(F::NAME).unwrap().config)(&value).unwrap()
    }

    /// Nothing to set up.
    fn prepare(_: &Scenario) {}

    /// Spawns the fixture with the change, after an unrelated task if the
    /// change adds one.
    fn build(sim: &mut Sim, _: &Scenario, (): ()) -> Self {
        if C::VARIANT == Variant::UnrelatedTask {
            sim.scheduler().spawn_root("unrelated", async {
                zakura_rt::time::sleep(Duration::from_millis(3)).await;
            });
        }
        Changed(F::spawn(sim, C::VARIANT), PhantomData)
    }

    /// The fixture's property, as the unchanged harness registers it.
    fn properties() -> Vec<Property<Self>> {
        vec![Property {
            id: PropertyId::new(F::NAME),
            claim: F::EXPECTED,
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: PropertyStatus::Active,
            check: no_race::<F, C>,
            witness: Some(TriggerHistory::Log(F::trigger_history)),
        }]
    }

    /// The fixture's view and the step loop's task observations.
    fn sources() -> Sources {
        Sources::observed(&[Source::View, Source::Schedule])
    }

    /// As the unchanged harness draws it.
    fn draw(prefix: &[u8]) -> Scenario {
        serde_json::from_value((harness::find(F::NAME).unwrap().draw)(prefix)).unwrap()
    }
}

/// The fixture's check: violated once its view shows the race after a step
/// that leaves no task running.
fn no_race<F: Fixture, C: Change>(cx: &mut CheckCtx<'_, Changed<F, C>>) -> Verdict {
    if cx.status == Status::Running {
        return Verdict::NotApplicable;
    }
    match F::failure(&cx.view(), cx.status) {
        Some(actual) => cx.violated(F::EXPECTED, actual),
        None => Verdict::Holds,
    }
}

/// `F` with each change, by the change's variant.
fn changes<F: Fixture>() -> [(Variant, HarnessEntry); 4] {
    [
        (ToFixed::VARIANT, harness::entry::<Changed<F, ToFixed>>()),
        (AddTask::VARIANT, harness::entry::<Changed<F, AddTask>>()),
        (
            AddNewKind::VARIANT,
            harness::entry::<Changed<F, AddNewKind>>(),
        ),
        (
            AddExistingKind::VARIANT,
            harness::entry::<Changed<F, AddExistingKind>>(),
        ),
    ]
}

/// Where a replay left the trace's schedule: the kind of divergence and its
/// step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    /// The recorded pick isn't enabled.
    Pick(u64),
    /// A different number of actions is enabled.
    Options(u64),
}

/// Where an Exact replay of `trace` on `entry`, as on a build that isn't
/// equivalent to the recording one, left the trace's schedule. Panics on
/// any other result: such a replay counts record differences and goes on,
/// so only a schedule divergence ends it.
fn left_on_changed_build(trace: &Trace, entry: &HarnessEntry) -> Left {
    let mut changed = trace.clone();
    changed.meta.build.binary = "0".repeat(64);
    changed.meta.build.dirty = true;
    let allow = ReplayOptions {
        allow_build_mismatch: true,
        ..ReplayOptions::default()
    };
    match replay_with(&changed, entry, allow) {
        Err(ReplayError::ScheduleDiverged(Divergence::Pick { step, .. })) => Left::Pick(step),
        Err(ReplayError::ScheduleDiverged(Divergence::Options { step, .. })) => Left::Options(step),
        other => panic!("{}: expected a divergence, got {other:?}", entry.name),
    }
}

/// The step of the first record an Exact replay of `trace` on `entry`, on
/// this build, found different. Panics on a verified replay.
fn first_difference(trace: &Trace, entry: &HarnessEntry) -> u64 {
    match replay_with(trace, entry, ReplayOptions::default()) {
        Err(ReplayError::Nondeterministic { step, .. }) => step,
        Err(ReplayError::ScheduleDiverged(Divergence::Options { step, .. })) => step,
        other => panic!("{}: expected a difference, got {other:?}", entry.name),
    }
}

/// Where Exact replays of `trace` on `F` with each change left the
/// schedule, by the change's variant. A replay on this build, which treats
/// any differing record as nondeterminism, stops at or before that step.
fn replayed_on_changes<F: Fixture>(trace: &Trace) -> Vec<(Variant, Left)> {
    changes::<F>()
        .into_iter()
        .map(|(variant, entry)| {
            let left = left_on_changed_build(trace, &entry);
            let (Left::Pick(step) | Left::Options(step)) = left;
            assert!(
                first_difference(trace, &entry) <= step,
                "{} {variant:?}",
                F::NAME
            );
            (variant, left)
        })
        .collect()
}

/// S4.5: an Exact replay of each saved schedule's trace on code with the
/// fix, an unrelated task, or an extra point of either kind reports where
/// it left the recorded schedule, never a verified replay. The unchanged
/// harness replays the same trace verified, so the change is what
/// diverges.
#[test]
fn changed_code_replay_reports_divergence() {
    use Left::{Options, Pick};
    use Variant::{ExtraPointExistingKind, ExtraPointNewKind, Fixed, UnrelatedTask};
    let expected = BTreeMap::from([
        (LostUpdate::NAME, [Pick(3), Options(1), Pick(3), Pick(5)]),
        (
            SnapshotReserve::NAME,
            [Options(6), Options(1), Pick(3), Pick(5)],
        ),
        (MissedNotify::NAME, [Pick(3), Options(1), Pick(3), Pick(5)]),
        (
            TimerCancel::NAME,
            [Pick(5), Options(1), Pick(2), Options(3)],
        ),
        (AbBa::NAME, [Options(5), Options(1), Pick(3), Options(6)]),
        (
            AbortWithoutJoin::NAME,
            [Pick(9), Options(1), Pick(3), Options(4)],
        ),
    ]);
    let root = TempDir::new().unwrap();
    for (name, trace) in record_all(root.path()) {
        assert!(replay(&trace, ReplayOptions::default()).unwrap().verified());
        let left = match name {
            LostUpdate::NAME => replayed_on_changes::<LostUpdate>(&trace),
            SnapshotReserve::NAME => replayed_on_changes::<SnapshotReserve>(&trace),
            MissedNotify::NAME => replayed_on_changes::<MissedNotify>(&trace),
            TimerCancel::NAME => replayed_on_changes::<TimerCancel>(&trace),
            AbBa::NAME => replayed_on_changes::<AbBa>(&trace),
            AbortWithoutJoin::NAME => replayed_on_changes::<AbortWithoutJoin>(&trace),
            other => panic!("{other}: not a race fixture"),
        };
        let want: Vec<(Variant, Left)> = [
            Fixed,
            UnrelatedTask,
            ExtraPointNewKind,
            ExtraPointExistingKind,
        ]
        .into_iter()
        .zip(expected[name])
        .collect();
        assert_eq!(left, want, "{name}");
    }
}
