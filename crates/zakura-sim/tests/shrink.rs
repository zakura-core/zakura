//! T3b: the shrinker reduces a failing trace to fewer non-default picks on
//! the same build, and `replay --explain` reads it back (traces.md, Trace
//! shrinking).
#![cfg(zakura_sim)]

use std::{
    cell::Cell,
    path::Path,
    process::Command,
    sync::{atomic::AtomicBool, OnceLock},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    action::Boundary,
    harness::{self, entry, run_harness, HarnessEntry, RecordInput, Subsystem},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Status, Verdict},
    record::{DrawRecord, Record, StepRecord},
    trace::{
        replay_explained_with, replay_with,
        shrink::{
            delete_picks, source, Counts, Stop, BEST_PLAN_FILE, ORIGINAL_DIR, REDUCED_DIR,
            REDUCTIONS_FILE, REPORT_FILE,
        },
        shrink_with, Candidate, ReplayError, ReplayOptions, ShrinkBudget, ShrinkError,
        ShrinkReport, Trace, TraceReader, TRACE_FILE,
    },
    ActionSource, By, HexU64, Identity, MemorySink, Observation, ObservationLog, Plan, PlanChoice,
    PlanInput, RunConfig, Sim, SkipScript,
};

/// The test harnesses' empty view.
macro_rules! no_view {
    ($harness:ty) => {
        impl HasView for $harness {
            type View = ();

            /// Nothing: the harness's checks read observations only.
            fn view(&self) {}
        }
    };
}

/// A test harness's fixed scenario.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Fixed {}

/// A run config with one worker, small stacks, and these step limits.
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

/// The complete trace in `dir`.
fn open(dir: &Path) -> Trace {
    TraceReader::open(dir).unwrap()
}

/// Records `harness` running `plan`.
fn record_script(harness: &HarnessEntry, plan: &Plan) -> TempDir {
    let dir = TempDir::new().unwrap();
    let scenario = json!({});
    let what = RecordInput::Script {
        scenario: &scenario,
        plan,
        derived_from: None,
    };
    harness::record(harness, what, dir.path()).unwrap();
    dir
}

// The stale-owner fixture: storage operation 1 belongs to the node's first
// owner. A reset, then a replacement owner, then the operation's late
// completion is the trigger history. The buggy store applies that late
// completion, which `STALE_INERT` forbids. Three peers ping the node and
// their pings are delivered: unrelated traffic the shrinker should drop.

/// The bug's property.
const STALE_INERT: PropertyId = PropertyId::new("test/stale-completion-inert");

/// A second property: a completion between the reset and the replacement.
/// A candidate that violates it instead is rejected.
const NO_ORPHAN: PropertyId = PropertyId::new("test/no-orphan-completion");

/// The peers sending unrelated traffic.
const PEERS: u64 = 3;

/// Pings per peer.
const PINGS: u64 = 4;

/// The owner events, as the trigger history reads them.
const RESET: &str = "reset";
const REPLACE: &str = "replace";
const LATE: &str = "late completion applied";
const ORPHAN: &str = "completion during reset";

/// The stale-owner fixture's actions.
#[derive(Default)]
struct Owners {
    reset: bool,
    replaced: bool,
    completed: bool,
    /// Pings each peer sent.
    pings: [u64; PEERS as usize],
    /// Pings delivered from each peer.
    delivered: [u64; PEERS as usize],
}

/// [`Owners`]' action indices.
const COMPLETE: u64 = 0;
const DO_RESET: u64 = 1;
const DO_REPLACE: u64 = 2;
const PING: u64 = 10;
const DELIVER: u64 = 20;

/// Peer `index`'s name, `p1` to `p3`.
fn peer(index: u64) -> String {
    format!("p{}", index + 1)
}

/// The link from peer `index` to the node.
fn link(index: u64) -> String {
    format!("{}->node", peer(index))
}

/// An owner event observation.
fn owner_event(event: &str) -> Observation {
    Observation::Fake {
        source: "owner".into(),
        event: event.into(),
    }
}

impl ActionSource for Owners {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.completed {
            out.push((Identity::storage("owner-store", 1, "success"), COMPLETE));
        }
        if !self.reset {
            out.push((Identity::peer("node", "reset"), DO_RESET));
        } else if !self.replaced {
            out.push((Identity::peer("node", "replace"), DO_REPLACE));
        }
        for index in 0..PEERS {
            // Peer indices are below 3.
            let i = index as usize;
            if self.pings[i] < PINGS {
                let action = format!("ping{}", self.pings[i] + 1);
                out.push((Identity::peer(&peer(index), &action), PING + index));
            }
            if self.delivered[i] < self.pings[i] {
                out.push((
                    Identity::deliver(&link(index), self.delivered[i] + 1),
                    DELIVER + index,
                ));
            }
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        match local {
            COMPLETE => {
                self.completed = true;
                let event = match (self.reset, self.replaced) {
                    (true, true) => LATE,
                    (true, false) => ORPHAN,
                    _ => "completion",
                };
                log.push(owner_event(event));
            }
            DO_RESET => {
                self.reset = true;
                log.push(owner_event(RESET));
            }
            DO_REPLACE => {
                self.replaced = true;
                log.push(owner_event(REPLACE));
            }
            _ => {
                let (index, what) = if local >= DELIVER {
                    (local - DELIVER, "deliver")
                } else {
                    (local - PING, "ping")
                };
                // Peer indices are below 3.
                let i = index as usize;
                let n = if what == "ping" {
                    self.pings[i] += 1;
                    self.pings[i]
                } else {
                    self.delivered[i] += 1;
                    self.delivered[i]
                };
                log.push(Observation::Fake {
                    source: peer(index),
                    event: format!("{what} {n}"),
                });
            }
        }
    }
}

/// True if this step's observations hold the owner event `event`.
fn observed(cx: &CheckCtx<'_, StaleOwner>, event: &str) -> bool {
    cx.observations().any(|obs| obs == &owner_event(event))
}

/// [`STALE_INERT`]'s check: violated when a late completion is applied.
fn stale_inert(cx: &mut CheckCtx<'_, StaleOwner>) -> Verdict {
    if observed(cx, LATE) {
        cx.violated("a stale owner's completion is inert", LATE)
    } else {
        Verdict::Holds
    }
}

/// [`NO_ORPHAN`]'s check: violated by a completion during a reset.
fn no_orphan(cx: &mut CheckCtx<'_, StaleOwner>) -> Verdict {
    if observed(cx, ORPHAN) {
        cx.violated("no completion lands during a reset", ORPHAN)
    } else {
        Verdict::Holds
    }
}

/// The stale-owner trigger history: the reset, then the replacement, then
/// the late completion.
fn reset_replace_late(log: &ObservationLog) -> bool {
    let at = |event: &str| {
        log.entries()
            .iter()
            .position(|entry| entry.obs == owner_event(event))
    };
    matches!(
        (at(RESET), at(REPLACE), at(LATE)),
        (Some(reset), Some(replace), Some(late)) if reset < replace && replace < late
    )
}

/// Any run reaches [`NO_ORPHAN`]'s trigger.
fn any_run(_: &ObservationLog) -> bool {
    true
}

/// The stale-owner fixture harness.
struct StaleOwner;

/// [`StaleOwner`]'s registry entry, which isn't in `HARNESSES`.
const STALE: HarnessEntry = entry::<StaleOwner>();

no_view!(StaleOwner);

thread_local! {
    /// Builds left before [`StaleOwner`] sets [`INTERRUPT`], if armed.
    static BUILDS_BEFORE_INTERRUPT: Cell<Option<u32>> = const { Cell::new(None) };
    /// The interrupt flag `interrupt_keeps_best` hands the shrinker.
    static INTERRUPT: AtomicBool = const { AtomicBool::new(false) };
}

impl Subsystem for StaleOwner {
    const NAME: &'static str = "test/stale-owner";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Fixed;
    type Prepared = ();

    /// Room for every action.
    fn config(_: &Fixed) -> RunConfig {
        limits(200, 10)
    }

    /// Nothing to set up.
    fn prepare(_: &Fixed) {}

    /// Adds the actions, and sets [`INTERRUPT`] once the armed count of
    /// builds runs out.
    fn build(sim: &mut Sim, _: &Fixed, (): ()) -> Self {
        BUILDS_BEFORE_INTERRUPT.with(|left| match left.get() {
            Some(0) => {
                INTERRUPT.with(|flag| flag.store(true, std::sync::atomic::Ordering::Relaxed))
            }
            Some(n) => left.set(Some(n - 1)),
            None => {}
        });
        sim.add_source(Box::new(Owners::default()));
        StaleOwner
    }

    /// The bug's property and the orphan property.
    fn properties() -> Vec<Property<Self>> {
        let property =
            |id, claim, check: fn(&mut CheckCtx<'_, Self>) -> Verdict, witness| Property {
                id,
                claim,
                kind: Kind::Safety,
                assumes: &[],
                needs: &[],
                status: Status::Active,
                check,
                witness: Some(witness),
            };
        vec![
            property(
                STALE_INERT,
                "a stale owner's completion is inert",
                stale_inert,
                reset_replace_late,
            ),
            property(
                NO_ORPHAN,
                "no completion lands during a reset",
                no_orphan,
                any_run,
            ),
        ]
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

/// The padded stale-owner failure: the peers' pings and deliveries, round
/// by round, with the reset after 3 picks and the replacement after 12, and
/// the late completion last.
fn padded_plan() -> Plan {
    let mut picks = Vec::new();
    for round in 1..=PINGS {
        for index in 0..PEERS {
            picks.push(Identity::peer(&peer(index), &format!("ping{round}")).to_string());
        }
        for index in 0..PEERS {
            picks.push(Identity::deliver(&link(index), round).to_string());
        }
    }
    picks.insert(12, Identity::peer("node", "replace").to_string());
    picks.insert(3, Identity::peer("node", "reset").to_string());
    picks.push(Identity::storage("owner-store", 1, "success").to_string());
    Plan {
        choices: picks.into_iter().map(PlanChoice::Pick).collect(),
    }
}

/// The padded stale-owner failure's trace.
fn padded() -> TempDir {
    record_script(&STALE, &padded_plan())
}

/// Shrinks the trace in `dir` into a new directory with `budget`.
fn shrunk(dir: &Path, budget: ShrinkBudget) -> (TempDir, ShrinkReport) {
    let out = TempDir::new().unwrap();
    let report = shrink_with(
        &open(dir),
        &STALE,
        budget,
        out.path(),
        &AtomicBool::new(false),
    )
    .unwrap();
    (out, report)
}

/// The non-default picks of `trace`, in order.
fn non_default_picks(trace: &Trace) -> Vec<String> {
    trace
        .steps
        .iter()
        .filter(|step| step.by == By::Bytes)
        .map(|step| step.pick.clone())
        .collect()
}

/// True if the steps of `trace` that pick the owner actions keep the
/// trigger history's order: the reset, the replacement, then the
/// completion.
fn keeps_owner_order(trace: &Trace) -> bool {
    let at = |pick: Identity| {
        trace
            .steps
            .iter()
            .position(|step| step.pick == pick.to_string())
    };
    matches!(
        (
            at(Identity::peer("node", "reset")),
            at(Identity::peer("node", "replace")),
            at(Identity::storage("owner-store", 1, "success")),
        ),
        (Some(reset), Some(replace), Some(late)) if reset < replace && replace < late
    )
}

#[test]
fn padded_stale_owner_shrinks_to_its_witness() {
    let dir = padded();
    let original = open(dir.path());
    let end = original.end.as_ref().unwrap();
    assert_eq!(
        end.violation.as_ref().map(|v| v.property.as_str()),
        Some(STALE_INERT.as_str()),
        "{end:?}"
    );
    assert!(keeps_owner_order(&original));
    let padding = non_default_picks(&original)
        .iter()
        .filter(|pick| !pick.starts_with("peer:node:"))
        .count();
    assert!(padding >= 20, "the original is padded: {padding} picks");

    let (out, report) = shrunk(dir.path(), ShrinkBudget::default());
    assert!(report.fixed_point, "{report:?}");
    let reduced = open(&out.path().join(REDUCED_DIR));
    assert_eq!(report.reduced.as_deref(), Some(reduced.id.as_str()));

    // Only the two owner picks the bug needs stay non-default. Round-robin
    // takes the storage completion first, so neither can be a default.
    assert_eq!(
        non_default_picks(&reduced),
        [
            Identity::peer("node", "reset").to_string(),
            Identity::peer("node", "replace").to_string()
        ]
    );
    assert!(keeps_owner_order(&reduced));
    let reduced_end = reduced.end.as_ref().unwrap();
    assert_eq!(
        reduced_end.violation.as_ref().map(|v| v.property.as_str()),
        Some(STALE_INERT.as_str())
    );
    // Deleting a pick deletes a choice, not its event: round-robin still
    // runs the peers' traffic, as default picks.
    assert_eq!(reduced_end.steps, end.steps, "{reduced_end:?}");

    // The reduced trace's run shows the trigger history.
    let replayed = replay_explained_with(&reduced, &STALE, ReplayOptions::default()).unwrap();
    assert!(replayed.0.verified(), "{:?}", replayed.0);
    let derived = reduced.meta.derived_from.as_ref().unwrap();
    assert_eq!(derived.trace, original.id);

    // The output keeps the original, the best plan and the reduction log.
    assert_eq!(
        std::fs::read(out.path().join(ORIGINAL_DIR).join(TRACE_FILE)).unwrap(),
        std::fs::read(dir.path().join(TRACE_FILE)).unwrap()
    );
    let best: Plan =
        serde_json::from_slice(&std::fs::read(out.path().join(BEST_PLAN_FILE)).unwrap()).unwrap();
    assert!(!best.choices.is_empty());
    let log = std::fs::read_to_string(out.path().join(REDUCTIONS_FILE)).unwrap();
    assert_eq!(log.lines().count(), report.accepted as usize);
    assert!(
        log.lines().any(|line| line.contains(r#""pass":3"#)),
        "{log}"
    );
    assert!(out.path().join(REPORT_FILE).is_file());
}

/// The candidate that runs `steps` after the trace's `init` draws, and its
/// records.
fn run_candidate<S: Subsystem<Scenario = Fixed>>(
    init: Vec<DrawRecord>,
    steps: Vec<Candidate>,
) -> (Vec<DrawRecord>, Vec<StepRecord>) {
    let mut sink = MemorySink::default();
    let input = PlanInput::Skip(SkipScript::new(init, steps));
    // A failed run still emits its records.
    let _ = run_harness::<S>(&Fixed {}, input, &mut sink, None);
    let mut init = Vec::new();
    let mut steps = Vec::new();
    for record in sink.records {
        match record {
            Record::Init(record) => init = record.draws,
            Record::Step(step) => steps.push(step),
            Record::Meta(_) | Record::End(_) => {}
        }
    }
    (init, steps)
}

/// One task that takes a two-branch unbiased `select!` three times, so each
/// pass draws below 2.
struct Selector;

/// [`Selector`]'s registry entry, which isn't in `HARNESSES`.
const SELECTOR: HarnessEntry = entry::<Selector>();

no_view!(Selector);

impl Subsystem for Selector {
    const NAME: &'static str = "test/selector";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Fixed;
    type Prepared = ();

    /// Room for the three passes.
    fn config(_: &Fixed) -> RunConfig {
        limits(100, 10)
    }

    /// Nothing to set up.
    fn prepare(_: &Fixed) {}

    /// Spawns the task.
    fn build(sim: &mut Sim, _: &Fixed, (): ()) -> Self {
        sim.scheduler().spawn_root("sel", async {
            for _ in 0..3 {
                zakura_rt::select! {
                    () = std::future::ready(()) => {}
                    () = std::future::ready(()) => {}
                }
                zakura_rt::task::yield_now().await;
            }
        });
        Selector
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

#[test]
fn skipped_pick_runs_the_default() {
    // At step 1 no reset has happened, so the replacement isn't enabled.
    // The default pick runs instead: round-robin's first action, the
    // storage completion.
    let replace = StepRecord {
        step: 1,
        pick: Identity::peer("node", "replace").to_string(),
        key: None,
        options: 5,
        by: By::Bytes,
        offset: None,
        draws: Vec::new(),
        obs: None,
    };
    let ping = StepRecord {
        pick: Identity::peer("p1", "ping1").to_string(),
        ..replace.clone()
    };
    let (_, steps) = run_candidate::<StaleOwner>(
        Vec::new(),
        vec![Candidate::Recorded(replace), Candidate::Recorded(ping)],
    );
    let completion = Identity::storage("owner-store", 1, "success").to_string();
    assert_eq!(
        (steps[0].pick.as_str(), steps[0].by),
        (completion.as_str(), By::Default)
    );
    // The skipped pick took its step: step 2 follows its own candidate,
    // where round-robin would take the reset.
    assert_eq!(
        (steps[1].pick.as_str(), steps[1].by),
        ("peer:p1:ping1", By::Bytes)
    );

    // A recorded pick that equals the round-robin pick counts as a default.
    let first = StepRecord {
        pick: completion.clone(),
        ..steps[0].clone()
    };
    let (_, steps) = run_candidate::<StaleOwner>(Vec::new(), vec![Candidate::Recorded(first)]);
    assert_eq!(
        (steps[0].pick.as_str(), steps[0].by),
        (completion.as_str(), By::Default)
    );

    // A draw answers from its record only if its identity and bound match.
    let (_, recorded) = run_candidate::<Selector>(Vec::new(), Vec::new());
    let step = recorded
        .iter()
        .find(|step| step.draws.iter().any(|d| d.bound.0 == 2))
        .expect("a step draws the select")
        .clone();
    let at = (step.step - 1) as usize;
    let (index, draw) = step
        .draws
        .iter()
        .enumerate()
        .find(|(_, d)| d.bound.0 == 2)
        .unwrap();
    assert_eq!((draw.value.0, draw.by), (0, By::Default), "{draw:?}");
    let with_draw = |draw: DrawRecord| {
        let mut candidates: Vec<Candidate> = recorded[..at]
            .iter()
            .cloned()
            .map(Candidate::Recorded)
            .collect();
        let mut step = step.clone();
        step.draws[index] = draw;
        candidates.push(Candidate::Recorded(step));
        let (_, steps) = run_candidate::<Selector>(Vec::new(), candidates);
        steps[at].draws[index].clone()
    };
    let other = DrawRecord {
        value: HexU64(1),
        by: By::Bytes,
        ..draw.clone()
    };
    let answered = with_draw(other.clone());
    assert_eq!((answered.value.0, answered.by), (1, By::Bytes));
    for mismatched in [
        DrawRecord {
            draw: format!("{}-renamed", other.draw),
            ..other.clone()
        },
        DrawRecord {
            bound: HexU64(3),
            ..other.clone()
        },
    ] {
        let answered = with_draw(mismatched);
        assert_eq!(
            (answered.value, answered.by),
            (draw.value, By::Default),
            "a mismatched draw gets the default answer"
        );
    }
}

#[test]
fn deletion_takes_later_same_source_picks() {
    let step = |pick: &str| {
        Candidate::Recorded(StepRecord {
            step: 0,
            pick: pick.to_owned(),
            key: None,
            options: 2,
            by: By::Bytes,
            offset: None,
            draws: Vec::new(),
            obs: None,
        })
    };
    let picks = [
        "peer:p1:ping1",
        "task:sel.1:poll#1",
        "peer:p1:ping2",
        "deliver:p1->node:1",
        "peer:p2:ping1",
        "deliver:p1->node:2",
        "drop:p1->node:3",
        "storage:store:7:fault",
        "storage:store:7:success",
        "storage:store:8:success",
    ];
    let mut steps: Vec<Candidate> = picks.iter().map(|pick| step(pick)).collect();
    steps.push(Candidate::Default);
    let remaining = |steps: &[Candidate]| -> Vec<String> {
        steps
            .iter()
            .map(|candidate| match candidate {
                Candidate::Recorded(step) => step.pick.clone(),
                Candidate::Default => "default".to_owned(),
            })
            .collect()
    };

    // A peer, a link and a storage operation each lose their later picks.
    assert_eq!(
        remaining(&delete_picks(&steps, &[0, 3, 7])),
        [
            "task:sel.1:poll#1",
            "peer:p2:ping1",
            "storage:store:8:success",
            "default"
        ]
    );
    // Earlier picks from the same source stay.
    assert_eq!(
        remaining(&delete_picks(&steps, &[2])),
        remaining(&[&steps[..2], &steps[3..]].concat())
    );
    assert_eq!(
        remaining(&delete_picks(&steps, &[5])),
        remaining(&[&steps[..5], &steps[7..]].concat())
    );
    assert_eq!(source("peer:p1:send:hello").as_deref(), Some("peer:p1"));
    assert_eq!(source("drop:p1->node:3").as_deref(), Some("link:p1->node"));
    assert_eq!(
        source("storage:header-store:7:success").as_deref(),
        Some("storage:header-store:7")
    );
    assert_eq!(source("task:sel.1:poll#1"), None);
    assert_eq!(source("advance-time:5"), None);
}

/// Set to a reduced trace's directory when this test binary runs as the
/// replay child.
const REDUCED_CHILD: &str = "ZAKURA_SIM_SHRUNK_TRACE";

/// Marks a verified replay in the child's output.
const VERIFIED: &str = "zakura-sim-shrunk-verified";

/// Replays the trace that [`REDUCED_CHILD`] names when run as the child,
/// and does nothing otherwise.
#[test]
fn shrunk_replay_child() {
    if let Some(dir) = std::env::var_os(REDUCED_CHILD) {
        let report = replay_with(&open(Path::new(&dir)), &STALE, ReplayOptions::default())
            .expect("the reduced trace replays");
        assert!(report.verified(), "{report:?}");
        #[allow(
            clippy::print_stdout,
            reason = "the parent test reads the child's stdout"
        )]
        {
            println!("\n{VERIFIED}");
        }
    }
}

#[test]
fn shrunk_replays_exact_with_fewer_non_default_picks() {
    let dir = padded();
    let (out, report) = shrunk(dir.path(), ShrinkBudget::default());
    let reduced = out.path().join(REDUCED_DIR);
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "shrunk_replay_child", "--nocapture"])
        .env(REDUCED_CHILD, &reduced)
        .output()
        .expect("the test binary reruns itself");
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(VERIFIED),
        "{output:?}"
    );
    let before = Counts::of(&open(dir.path())).non_default;
    let after = Counts::of(&open(&reduced)).non_default;
    assert!(after < before, "{after} of {before}");
    assert!(report.fixed_point, "{report:?}");
}

/// The host clock, as nanoseconds since this process first read it.
fn host_nanos() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos()
}

/// A source that ticks, then reads the host clock into an observation: a
/// planted nondeterminism. The read violates [`CLOCK_READ`].
struct Clock(Cell<u32>);

impl ActionSource for Clock {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        match self.0.get() {
            0..=1 => out.push((Identity::peer("clock", &format!("tick{}", self.0.get())), 0)),
            2 => out.push((Identity::peer("clock", "read"), 0)),
            _ => {}
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let n = self.0.get();
        let event = match n {
            0..=1 => format!("tick {n}"),
            _ => format!("read {}", host_nanos()),
        };
        self.0.set(n + 1);
        log.push(Observation::Fake {
            source: "clock".into(),
            event,
        });
    }
}

/// Violated at the host-clock read.
const CLOCK_READ: PropertyId = PropertyId::new("test/no-clock-read");

/// [`CLOCK_READ`]'s check.
fn no_clock_read(cx: &mut CheckCtx<'_, PlantedClock>) -> Verdict {
    let read = cx
        .observations()
        .any(|obs| matches!(obs, Observation::Fake { event, .. } if event.starts_with("read")));
    if read {
        cx.violated("no host-clock read", "read the host clock")
    } else {
        Verdict::Holds
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
    type Prepared = ();

    /// Room for the three actions.
    fn config(_: &Fixed) -> RunConfig {
        limits(100, 10)
    }

    /// Nothing to set up.
    fn prepare(_: &Fixed) {}

    /// Adds the clock.
    fn build(sim: &mut Sim, _: &Fixed, (): ()) -> Self {
        sim.add_source(Box::new(Clock(Cell::new(0))));
        PlantedClock
    }

    /// The read's property.
    fn properties() -> Vec<Property<Self>> {
        vec![Property {
            id: CLOCK_READ,
            claim: "no host-clock read",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[],
            status: Status::Active,
            check: no_clock_read,
            witness: Some(any_run),
        }]
    }

    /// The fixed scenario.
    fn draw(_: &[u8]) -> Fixed {
        Fixed {}
    }
}

#[test]
fn refuses_nondeterministic() {
    let clock = entry::<PlantedClock>();
    let dir = TempDir::new().unwrap();
    harness::record(&clock, RecordInput::Input(&[]), dir.path()).unwrap();
    let recorded = std::fs::read(dir.path().join(TRACE_FILE)).unwrap();
    std::thread::sleep(Duration::from_micros(10));
    let out = TempDir::new().unwrap();
    let result = shrink_with(
        &open(dir.path()),
        &clock,
        ShrinkBudget::default(),
        out.path(),
        &AtomicBool::new(false),
    );
    match result {
        Err(ShrinkError::Confirm(ReplayError::Nondeterministic { step, field })) => {
            assert_eq!((step, field), (3, "obs"));
        }
        other => panic!("expected the Confirm pass to refuse, got {other:?}"),
    }
    // The trace is kept as it was, and nothing is written.
    assert_eq!(
        std::fs::read(dir.path().join(TRACE_FILE)).unwrap(),
        recorded
    );
    assert_eq!(std::fs::read_dir(out.path()).unwrap().count(), 0);
}

#[test]
fn interrupt_keeps_best() {
    let dir = padded();
    let recorded = std::fs::read(dir.path().join(TRACE_FILE)).unwrap();
    // The confirm replay and the baseline build twice, then candidates
    // build once each: the flag is set while the fifth candidate builds.
    BUILDS_BEFORE_INTERRUPT.set(Some(6));
    let out = TempDir::new().unwrap();
    let report = INTERRUPT.with(|flag| {
        shrink_with(
            &open(dir.path()),
            &STALE,
            ShrinkBudget::default(),
            out.path(),
            flag,
        )
    });
    BUILDS_BEFORE_INTERRUPT.set(None);
    let report = report.unwrap();
    assert_eq!(report.stopped, Some(Stop::Interrupted), "{report:?}");
    assert!(!report.fixed_point);
    assert_eq!(report.candidates, 5, "{report:?}");

    // The original is untouched, and the output keeps a copy of it.
    assert_eq!(
        std::fs::read(dir.path().join(TRACE_FILE)).unwrap(),
        recorded
    );
    assert_eq!(
        std::fs::read(out.path().join(ORIGINAL_DIR).join(TRACE_FILE)).unwrap(),
        recorded
    );
    // The best candidate so far is kept as a plan and re-recorded.
    assert!(out.path().join(BEST_PLAN_FILE).is_file());
    let reduced = open(&out.path().join(REDUCED_DIR));
    let after = report.after.unwrap();
    assert_eq!(after, Counts::of(&reduced));
    assert!(after.non_default <= report.before.non_default);
    assert!(keeps_owner_order(&reduced));
}

#[test]
fn shrink_report_counts_executed_events() {
    let dir = padded();
    let original = open(dir.path());
    let (out, report) = shrunk(dir.path(), ShrinkBudget::default());
    let reduced = open(&out.path().join(REDUCED_DIR));

    // Counted by hand: every pick the script made is non-default, except
    // the last, which was forced; the original has no draws.
    let picks = |trace: &Trace| {
        trace
            .steps
            .iter()
            .filter(|step| step.by == By::Bytes)
            .count() as u64
    };
    assert!(original.steps.iter().all(|step| step.draws.is_empty()));
    assert_eq!(
        report.before,
        Counts {
            non_default: picks(&original),
            events: original.end.as_ref().unwrap().steps,
        }
    );
    assert_eq!(
        report.after,
        Some(Counts {
            non_default: picks(&reduced),
            events: reduced.end.as_ref().unwrap().steps,
        })
    );
    assert!(report.fixed_point && report.stopped.is_none(), "{report:?}");
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.path().join(REPORT_FILE)).unwrap()).unwrap();
    assert_eq!(written["fixed_point"], json!(true));
    assert_eq!(
        written["after"]["events"],
        json!(report.after.unwrap().events)
    );

    // A shrink cut short by its candidate limit says pass 4 reached no
    // fixed point.
    let (_, limited) = shrunk(
        dir.path(),
        ShrinkBudget {
            candidates: 3,
            ..ShrinkBudget::default()
        },
    );
    assert_eq!(limited.stopped, Some(Stop::CandidateLimit));
    assert!(!limited.fixed_point);
    assert_eq!(limited.candidates, 3);
}

#[test]
fn explain_marks_non_default_picks() {
    let dir = padded();
    let (out, _) = shrunk(dir.path(), ShrinkBudget::default());
    let reduced = open(&out.path().join(REDUCED_DIR));
    let (report, text) = replay_explained_with(&reduced, &STALE, ReplayOptions::default()).unwrap();
    assert!(report.verified());
    let marked: Vec<&str> = text.lines().filter(|line| line.contains(" * ")).collect();
    assert_eq!(marked.len(), 2, "{text}");
    assert!(marked[0].contains("peer:node:reset"), "{text}");
    assert!(marked[1].contains("peer:node:replace"), "{text}");
    assert!(
        text.contains(r#"{"obs":"fake","source":"owner","event":"late completion applied"}"#),
        "{text}"
    );
    assert!(
        text.contains("test/stale-completion-inert violated"),
        "{text}"
    );

    // A task pick shows its task's name and point, and its draws.
    let selector = TempDir::new().unwrap();
    harness::record(&SELECTOR, RecordInput::Input(&[1]), selector.path()).unwrap();
    let (_, text) =
        replay_explained_with(&open(selector.path()), &SELECTOR, ReplayOptions::default()).unwrap();
    assert!(text.contains("task sel.1 at "), "{text}");
    assert!(text.contains("draw sel.1:select#1 = "), "{text}");
}
