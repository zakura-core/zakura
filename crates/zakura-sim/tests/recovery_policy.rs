//! fuzz-12 item 5's decider: once a source starts recovery, picks go least
//! recently picked first and draws rotate per call site, recorded `recovery`, and
//! the input is no longer read. A test harness stands in for the storage
//! harness's `Recover` step, which comes in a later PR: one source action
//! starts recovery, three tasks spin, and a fourth alternates two
//! two-branch `select!` sites.
#![cfg(zakura_sim)]

use std::sync::atomic::AtomicBool;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    action::Boundary,
    harness::{self, entry, run_harness, HarnessEntry, HarnessError, RecordInput, Subsystem},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Status, TriggerHistory, Verdict},
    record::{DrawRecord, Record, StepRecord},
    trace::{
        replay_with, shrink::REDUCED_DIR, shrink_with, Candidate, ReplayOptions, ShrinkBudget,
        Trace, TraceReader,
    },
    ActionSource, By, HarnessFailure, Identity, MemorySink, Observation, ObservationLog, Plan,
    PlanInput, RunConfig, RunReport, Sim, SkipScript,
};

/// Whether the switch's action starts recovery, and whether an alarm action
/// follows it. A scenario that doesn't recover is the control: the same
/// run, still reading its input.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Mode {
    recover: bool,
    #[serde(default)]
    alarm: bool,
}

/// The property the alarm violates.
const NO_ALARM: PropertyId = PropertyId::new("test/no-alarm");

/// The switch's or the alarm's logged event.
fn event(what: &str) -> Observation {
    Observation::Fake {
        source: "switch".into(),
        event: what.into(),
    }
}

/// [`NO_ALARM`]'s check: violated once the alarm rings.
fn no_alarm(cx: &mut CheckCtx<'_, Recovering>) -> Verdict {
    if cx.observations().any(|obs| obs == &event("alarm")) {
        cx.violated("no alarm rings", "alarm")
    } else {
        Verdict::Holds
    }
}

/// [`NO_ALARM`]'s trigger history: the switch, then the alarm.
fn switch_then_alarm(log: &ObservationLog) -> bool {
    let at = |what| {
        log.entries()
            .iter()
            .position(|entry| entry.obs == event(what))
    };
    matches!((at("recover"), at("alarm")), (Some(a), Some(b)) if a < b)
}

/// The test harness.
struct Recovering;

/// [`Recovering`]'s registry entry, which isn't in `HARNESSES`.
const RECOVERING: HarnessEntry = entry::<Recovering>();

impl HasView for Recovering {
    type View = ();

    /// Nothing: the tests read records only.
    fn view(&self) {}
}

/// The source with the action that starts recovery, and in alarm mode an
/// alarm action offered once the switch is picked.
struct Switch {
    recover: bool,
    alarm: bool,
    picked: bool,
}

/// The switch's action.
fn recover() -> Identity {
    Identity::peer("node", "recover")
}

impl ActionSource for Switch {
    /// The switch until it's picked, then the alarm if there is one.
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.picked {
            out.push((recover(), 0));
        } else if self.alarm {
            out.push((Identity::peer("node", "alarm"), 1));
        }
    }

    /// No instants matter.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Notes the switch's pick, or rings the alarm.
    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        if local == 0 {
            self.picked = true;
            log.push(event("recover"));
        } else {
            log.push(event("alarm"));
        }
    }

    /// True once picked, unless this is the control.
    fn recovering(&self) -> bool {
        self.recover && self.picked
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut zakura_sim::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

impl Subsystem for Recovering {
    const NAME: &'static str = "test/recovering";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Mode;
    type Prepared = ();

    /// One worker and a budget long enough to see many rounds.
    fn config(_: &Mode) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 200,
            drain_bound: 20,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Mode) {}

    /// Adds the switch, three spinners and the selecting task.
    fn build(sim: &mut Sim, mode: &Mode, (): ()) -> Self {
        sim.add_source(Box::new(Switch {
            recover: mode.recover,
            alarm: mode.alarm,
            picked: false,
        }));
        for name in ["spin-a", "spin-b", "spin-c"] {
            sim.scheduler().spawn_root(name, async {
                loop {
                    zakura_rt::task::yield_now().await;
                }
            });
        }
        // Site A then site B, forever, so the task's select draws
        // alternate between the two sites: odd ordinals are A's.
        sim.scheduler().spawn_root("sel", async {
            loop {
                zakura_rt::select! {
                    () = std::future::ready(()) => {}
                    () = std::future::ready(()) => {}
                }
                zakura_rt::task::yield_now().await;
                zakura_rt::select! {
                    () = std::future::ready(()) => {}
                    () = std::future::ready(()) => {}
                }
                zakura_rt::task::yield_now().await;
            }
        });
        Recovering
    }

    /// [`NO_ALARM`], which only the alarm mode can violate.
    fn properties() -> Vec<Property<Self>> {
        vec![Property {
            id: NO_ALARM,
            claim: "no alarm rings",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[],
            status: Status::Active,
            check: no_alarm,
            witness: Some(TriggerHistory::Log(switch_then_alarm)),
        }]
    }

    /// The scenario that starts recovery.
    fn draw(_: &[u8]) -> Mode {
        Mode {
            recover: true,
            alarm: false,
        }
    }
}

/// A run's init draws and step records.
struct Run {
    init: Vec<DrawRecord>,
    steps: Vec<StepRecord>,
}

impl Run {
    /// The step that picked the switch, from 1, if one did.
    fn switch(&self) -> Option<u64> {
        self.steps
            .iter()
            .find(|step| step.pick == recover().to_string())
            .map(|step| step.step)
    }

    /// Every pick and draw, in order, as text.
    fn history(&self) -> Vec<String> {
        self.steps
            .iter()
            .flat_map(|step| {
                std::iter::once(format!("{} {}", step.step, step.pick)).chain(
                    step.draws
                        .iter()
                        .map(|draw| format!("  {} {}", draw.draw, draw.value)),
                )
            })
            .collect()
    }

    /// The `sel` task's select draws: (ordinal, value, by).
    fn selects(&self) -> Vec<(u32, u64, By)> {
        self.selects_through(u64::MAX)
    }

    /// [`selects`](Self::selects) of the steps through `last`.
    fn selects_through(&self, last: u64) -> Vec<(u32, u64, By)> {
        self.steps
            .iter()
            .filter(|step| step.step <= last)
            .flat_map(|step| &step.draws)
            .filter_map(|draw| {
                let ordinal = draw.draw.strip_prefix("sel.1:select#")?;
                Some((ordinal.parse().unwrap(), draw.value.0, draw.by))
            })
            .collect()
    }
}

/// The report of a run that `error` says ended at its drain's bound, which
/// every run of [`Recovering`]'s endless spinners does.
fn drain_bound(error: HarnessError) -> Box<RunReport> {
    match error {
        HarnessError::Run {
            failure: HarnessFailure::DrainBound { .. },
            report,
        } => report,
        error => panic!("{error}"),
    }
}

/// Runs `mode` under `input`.
fn run(mode: bool, input: PlanInput) -> Run {
    let mut sink = MemorySink::default();
    let mode = Mode {
        recover: mode,
        alarm: false,
    };
    // The spinners never stop, so the drain ends at its bound, a harness
    // failure these tests don't read: they read the picks and draws.
    if let Err(error) = run_harness::<Recovering>(&mode, input, &mut sink, None) {
        drain_bound(error);
    }
    let mut run = Run {
        init: Vec::new(),
        steps: Vec::new(),
    };
    for record in sink.records {
        match record {
            Record::Init(init) => run.init = init.draws,
            Record::Step(step) => run.steps.push(step),
            Record::Meta(_) | Record::End(_) => {}
        }
    }
    run
}

/// A first byte that picks the switch at step 1, then `tail`.
fn switch_first(tail: &[u8]) -> PlanInput {
    let first = (0..=u8::MAX)
        .find(|&byte| run(true, Plan::from_fuzz(&[byte])).switch() == Some(1))
        .expect("some first byte picks the switch");
    Plan::from_fuzz(&[&[first], tail].concat())
}

/// Byte inputs for the sweeps, the same on every call.
fn inputs() -> Vec<PlanInput> {
    (0u8..24)
        .map(|n| {
            let len = usize::from(n) * 3;
            Plan::from_fuzz(
                &(0..len)
                    .map(|i| (i as u8).wrapping_mul(n).wrapping_add(7))
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

/// Every pick and draw after the switch is the policy's, and none before it
/// is.
#[test]
fn every_choice_after_the_switch_is_the_policys() {
    for input in inputs() {
        let run = run(true, input);
        let switch = run.switch().expect("round-robin reaches the switch");
        for step in &run.steps {
            let draws = step.draws.iter().map(|draw| draw.by);
            if step.step <= switch {
                assert_ne!(step.by, By::Recovery, "step {}", step.step);
                assert!(
                    draws.clone().all(|by| by != By::Recovery),
                    "step {}",
                    step.step
                );
            } else {
                // `sel` stopped mid-poll is the only enabled action, so its
                // pick is forced. Every other pick is the policy's.
                let by = if step.options == 1 {
                    By::Forced
                } else {
                    By::Recovery
                };
                assert_eq!(step.by, by, "step {}", step.step);
                assert!(
                    draws.clone().all(|by| by == By::Recovery),
                    "step {}",
                    step.step
                );
            }
        }
    }
}

/// Bytes after the switch don't change the run, though the same bytes
/// change the control's.
#[test]
fn the_input_after_the_switch_is_ignored() {
    let tails: [&[u8]; 3] = [
        &[],
        &[0; 128],
        &[3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9],
    ];
    let recovering: Vec<_> = tails
        .iter()
        .map(|tail| run(true, switch_first(tail)).history())
        .collect();
    assert!(recovering.windows(2).all(|w| w[0] == w[1]));
    let control: Vec<_> = tails
        .iter()
        .map(|tail| run(false, switch_first(tail)).history())
        .collect();
    assert!(control[0] != control[1] && control[1] != control[2]);
}

/// After the switch each task runs in turn. A spinner yields straight
/// back, while `sel` takes a second, forced step at its `select!`, where it
/// holds the one worker and the spinners aren't enabled. Their stamps
/// survive that step, so once every task has run, every window of five
/// steps runs each one again.
#[test]
fn every_task_runs_within_a_round() {
    const TASKS: [&str; 4] = ["spin-a.1", "spin-b.1", "spin-c.1", "sel.1"];
    let runs = |step: &StepRecord, task: &str| step.pick.starts_with(&format!("task:{task}:"));
    for input in inputs() {
        let run = run(true, input);
        let switch = run.switch().expect("round-robin reaches the switch");
        let live: Vec<&StepRecord> = run
            .steps
            .iter()
            .filter(|s| s.step > switch && s.step <= 200)
            .collect();
        // Every task is enabled within two picks of the switch, and the
        // bound gives each a turn within a round of that.
        let warm = TASKS
            .iter()
            .map(|task| live.iter().position(|s| runs(s, task)).unwrap())
            .max()
            .unwrap();
        assert!(warm < 8, "{warm}");
        for window in live[warm..].windows(5) {
            for task in TASKS {
                assert!(
                    window.iter().any(|s| runs(s, task)),
                    "{task} doesn't run in steps {}..={}",
                    window[0].step,
                    window[4].step
                );
            }
        }
    }
}

/// With the input exhausted, the shared per-kind ordinal gives site A
/// branch 0 and site B branch 1 every time, through the live phase. After
/// the switch, or the drain's start, each site rotates through both
/// branches on its own.
#[test]
fn draws_rotate_per_call_site_after_the_switch() {
    let control = run(false, Plan::from_fuzz(&[]));
    let budget = Recovering::config(&Mode {
        recover: false,
        alarm: false,
    })
    .budget;
    for (ordinal, value, by) in control.selects_through(budget) {
        assert_eq!(by, By::Default);
        assert_eq!(
            value,
            u64::from(ordinal % 2 == 0),
            "site {}",
            if ordinal % 2 == 1 { "A" } else { "B" }
        );
    }
    let recovering = run(true, Plan::from_fuzz(&[]));
    let after: Vec<_> = recovering
        .selects()
        .into_iter()
        .filter(|(_, _, by)| *by == By::Recovery)
        .collect();
    assert!(after.len() > 8, "{after:?}");
    for site in [1, 0] {
        let values: Vec<u64> = after
            .iter()
            .filter(|(n, _, _)| n % 2 == site)
            .map(|(_, v, _)| *v)
            .collect();
        let rotating: Vec<u64> = (0..values.len() as u64).map(|n| n % 2).collect();
        assert_eq!(
            values,
            rotating,
            "site {}",
            if site == 1 { "A" } else { "B" }
        );
    }
}

/// A recovery run replays Exact from its trace, and its resolved script and
/// a skip script of its own steps rerun it.
#[test]
fn a_recovery_run_replays() {
    let bytes = [5, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let dir = TempDir::new().unwrap();
    harness::record(
        &RECOVERING,
        RecordInput::Plan {
            scenario: &json!({"recover": true}),
            plan: &bytes,
        },
        dir.path(),
    )
    .unwrap();
    let trace = TraceReader::open(dir.path()).unwrap();
    assert!(trace.steps.iter().any(|step| step.by == By::Recovery));
    let replayed = replay_with(&trace, &RECOVERING, ReplayOptions::default()).unwrap();
    assert!(replayed.verified(), "{replayed:?}");

    let original = run(true, Plan::from_fuzz(&bytes));
    let mut sink = MemorySink::default();
    let mode = Mode {
        recover: true,
        alarm: false,
    };
    let report = match run_harness::<Recovering>(&mode, Plan::from_fuzz(&bytes), &mut sink, None) {
        Ok(report) => report,
        Err(error) => *drain_bound(error),
    };
    assert_eq!(
        run(true, PlanInput::Script(report.resolved)).history(),
        original.history()
    );
    let skip = SkipScript::new(
        original.init.clone(),
        original
            .steps
            .iter()
            .cloned()
            .map(Candidate::Recorded)
            .collect(),
    );
    assert_eq!(
        run(true, PlanInput::Skip(skip)).history(),
        original.history()
    );
}

/// Records the alarm mode under `bytes` into a new directory.
fn record_alarm(bytes: &[u8]) -> TempDir {
    let dir = TempDir::new().unwrap();
    harness::record(
        &RECOVERING,
        RecordInput::Plan {
            scenario: &json!({"recover": true, "alarm": true}),
            plan: bytes,
        },
        dir.path(),
    )
    .unwrap();
    dir
}

/// `trace`'s picks that bytes chose.
fn chosen(trace: &Trace) -> Vec<&str> {
    trace
        .steps
        .iter()
        .filter(|step| step.by == By::Bytes)
        .map(|step| step.pick.as_str())
        .collect()
}

/// The shrinker keeps recovery choices as defaults: it drops the bytes
/// picks before the switch, keeps the policy's picks between the switch and
/// the alarm, and the reduced trace still replays Exact.
#[test]
fn a_recovery_trace_shrinks_and_replays() {
    // The first bytes that run the tasks for a while before the switch.
    let (_dir, original) = (1u8..)
        .map(|k| {
            let bytes: Vec<u8> = (0..40u8)
                .map(|n| n.wrapping_mul(k).wrapping_add(3))
                .collect();
            let dir = record_alarm(&bytes);
            let trace = TraceReader::open(dir.path()).unwrap();
            (dir, trace)
        })
        .find(|(_, trace)| chosen(trace).len() >= 8)
        .unwrap();
    let end = original.end.as_ref().unwrap();
    assert_eq!(
        end.violation.as_ref().map(|v| v.property.as_str()),
        Some(NO_ALARM.as_str())
    );
    let alarm = Identity::peer("node", "alarm").to_string();
    let rang = original.steps.iter().find(|s| s.pick == alarm).unwrap();
    assert_eq!(rang.by, By::Recovery, "the policy picks the alarm");

    let out = TempDir::new().unwrap();
    let report = shrink_with(
        &original,
        &RECOVERING,
        ShrinkBudget::default(),
        out.path(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(report.fixed_point, "{report:?}");
    let reduced = TraceReader::open(&out.path().join(REDUCED_DIR)).unwrap();
    assert!(
        chosen(&reduced).len() < chosen(&original).len(),
        "{:?}",
        chosen(&reduced)
    );
    assert!(reduced.steps.iter().any(|s| s.by == By::Recovery));
    assert_eq!(
        reduced
            .end
            .as_ref()
            .unwrap()
            .violation
            .as_ref()
            .map(|v| v.property.as_str()),
        Some(NO_ALARM.as_str())
    );
    let replayed = replay_with(&reduced, &RECOVERING, ReplayOptions::default()).unwrap();
    assert!(replayed.verified(), "{replayed:?}");
}
