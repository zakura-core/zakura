//! hdr-6.0: obligations that view-side facts answer, the deferred deadline,
//! and trigger histories that read the view.
#![cfg(zakura_sim)]

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_rt::{collections::HashMap, sync::Mutex, task, time};
use zakura_scheduler::SimInstant;
use zakura_sim::NaReason;
use zakura_sim::{
    fake::FakeStorage,
    harness::{run_harness, Subsystem},
    property::{
        Assume, Capture, CheckCtx, CheckEvent, FactAnswer, HasView, Kind, Lifecycle, LivenessBound,
        Obligation, ObligationKey, Property, PropertyId, Registry, RegistryError, Source, Sources,
        Status, Subject, TriggerHistory, Verdict,
    },
    witness::{judge_report, witness, Expect, WitnessError, WitnessScenario},
    Checker, MemorySink, ObservationLog, Outcome, Plan, PropertyOutcome, PropertyReport, RunConfig,
    RunEnd, RunReport, Sim, StepCtx, Violation,
};

/// A view-side fact the harness's task records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Name {
    /// Opens [`SERVED`]'s obligation after its own stamp.
    Wait,
    /// Opens [`NEXT`]'s obligation, for later steps' facts.
    WaitNext,
    /// Opens [`OBSERVED`]'s obligation, which every observation matches.
    WaitObserved,
    /// Discharges an open obligation.
    Done,
    /// Defers an open obligation to this many milliseconds after the start.
    Defer(u64),
}

/// The facts so far, with their stamps.
type Facts = Vec<(u64, Name)>;

/// One step of the harness's task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Act {
    /// Records a view-side fact.
    Fact(Name),
    /// Ends the task's poll, so later facts land in a later step.
    Yield,
    /// Sleeps this many milliseconds.
    Sleep(u64),
    /// Takes a lock, a preemption point inside the poll.
    Point,
    /// Waits for a fake storage operation, which completes within 1 ms.
    /// The scheduler alone looks deadlocked meanwhile.
    Store,
    /// Breaks the harness's evidence in this way.
    Break(Broken),
    /// Spawns a holder that yields holding a lock and two waiters that park
    /// on it, one per worker, so the holder is ready with no worker.
    Starve,
}

/// How a harness's evidence breaks. Each way, its view panics if read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Broken {
    /// Its stamps go backward.
    Stamps,
    /// It loses its snapshot.
    Snapshot,
    /// It's released when the run ends, as a harness may in `ended`.
    Released,
}

/// The task's acts, the run's live step budget, its drain bound, and
/// whether its evidence is released when the run ends.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Script {
    acts: Vec<Act>,
    budget: u64,
    drain: u64,
    release_at_end: bool,
}

/// One task that records stamped facts, as a tap records owner facts.
struct Stamped {
    facts: Rc<RefCell<Facts>>,
    broken: Rc<Cell<Option<Broken>>>,
    release_at_end: bool,
}

impl HasView for Stamped {
    type View = Facts;

    /// Every fact so far, with its stamp.
    ///
    /// # Panics
    ///
    /// Once the evidence broke.
    fn view(&self) -> Self::View {
        assert_eq!(
            self.broken.get(),
            None,
            "the view is unusable once its evidence broke"
        );
        self.facts.borrow().clone()
    }

    /// Releases the evidence if the script says to.
    fn ended(&self, _: &RunEnd) {
        if self.release_at_end {
            self.broken.set(Some(Broken::Released));
        }
    }

    /// Live, until the snapshot is lost.
    fn captured(&self) -> Capture {
        match self.broken.get() {
            Some(Broken::Snapshot) => Capture::Missing,
            _ => Capture::Live,
        }
    }

    /// The stamps after `after`, or a stamp that goes backward once the
    /// stamps broke.
    ///
    /// # Panics
    ///
    /// Once the snapshot is lost, which the stamps come from.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        assert_ne!(
            self.broken.get(),
            Some(Broken::Snapshot),
            "the stamps need the snapshot"
        );
        if self.broken.get() == Some(Broken::Stamps) {
            return Some(vec![0]);
        }
        Some(
            self.facts
                .borrow()
                .iter()
                .map(|(stamp, _)| *stamp)
                .filter(|stamp| after.is_none_or(|after| *stamp > after))
                .collect(),
        )
    }
}

/// `wait` opens an obligation after its stamp, and `done` discharges it.
const SERVED: &str = "test/wait-served";
/// `wait-next` opens one that only facts from later steps answer.
const NEXT: &str = "test/wait-next-served";
/// `wait-observed` opens one whose `discharged_by` matches everything.
const OBSERVED: &str = "test/wait-observed-served";
/// [`SERVED`] under an assumption no scenario declares.
const UNDECLARED: &str = "test/wait-served-if-honest";
/// Always holds, with a trigger history that draws.
const DRAWING: &str = "test/wait-served-drawing";

impl Subsystem for Stamped {
    const NAME: &'static str = "test/view-facts";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Script;
    type Prepared = ();

    fn config(script: &Script) -> RunConfig {
        RunConfig {
            budget: script.budget,
            drain_bound: script.drain,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Script) {}

    fn build(sim: &mut Sim, script: &Script, (): ()) -> Self {
        let facts = Rc::new(RefCell::new(Vec::new()));
        let task_facts = Rc::clone(&facts);
        let acts = script.acts.clone();
        let lock = Mutex::new(());
        let broken = Rc::new(Cell::new(None));
        let task_broken = Rc::clone(&broken);
        let storage = FakeStorage::new("store", Duration::from_millis(1));
        sim.add_source(Box::new(storage.clone()));
        // `Starve`'s tasks: a holder that takes a lock and yields holding
        // it, and two waiters that park on it once it's held, one per
        // worker.
        let starve = zakura_rt::sync::CancellationToken::new();
        if script.acts.contains(&Act::Starve) {
            let lock = Rc::new(Mutex::new(()));
            let held = zakura_rt::sync::CancellationToken::new();
            let (start, taken, holder_lock) = (starve.clone(), held.clone(), Rc::clone(&lock));
            sim.scheduler().spawn_root("holder", async move {
                start.cancelled().await;
                #[allow(clippy::await_holding_lock)]
                {
                    let _guard = holder_lock.lock().expect("no task panics holding it");
                    taken.cancel();
                    task::yield_now().await;
                }
            });
            for _ in 0..2 {
                let (held, lock) = (held.clone(), Rc::clone(&lock));
                sim.scheduler().spawn_root("waiter", async move {
                    held.cancelled().await;
                    drop(lock.lock().expect("no task panics holding it"));
                });
            }
        }
        sim.scheduler().spawn_root("stamper", async move {
            for act in acts {
                match act {
                    Act::Fact(name) => {
                        let mut facts = task_facts.borrow_mut();
                        let stamp =
                            u64::try_from(facts.len()).expect("a test records few facts") + 1;
                        facts.push((stamp, name));
                    }
                    Act::Yield => task::yield_now().await,
                    Act::Sleep(ms) => time::sleep(Duration::from_millis(ms)).await,
                    Act::Point => drop(lock.lock().expect("no task panics holding it")),
                    Act::Store => drop(storage.submit().await),
                    Act::Break(how) => task_broken.set(Some(how)),
                    Act::Starve => starve.cancel(),
                }
            }
        });
        Stamped {
            facts,
            broken,
            release_at_end: script.release_at_end,
        }
    }

    fn sources() -> Sources {
        Sources::observed(&[Source::Schedule, Source::View])
    }

    fn properties() -> Vec<Property<Self>> {
        let served = |id, check| Property {
            id: PropertyId::new(id),
            claim: "every wait is answered before quiescence",
            kind: Kind::Liveness {
                bound: LivenessBound::BeforeQuiescence,
            },
            assumes: &[],
            needs: &[Source::Schedule, Source::View],
            status: Status::Active,
            check,
            witness: Some(TriggerHistory::View(waited)),
        };
        vec![
            served(SERVED, waits),
            served(NEXT, waits_next),
            served(OBSERVED, waits_observed),
            Property {
                assumes: &[Assume::HonestPeer],
                ..served(UNDECLARED, waits)
            },
            Property {
                witness: Some(TriggerHistory::View(drew_and_waited)),
                ..served(DRAWING, |_| Verdict::Holds)
            },
        ]
    }

    fn draw(_: &[u8]) -> Script {
        Script {
            acts: Vec::new(),
            budget: 50,
            drain: 10,
            release_at_end: false,
        }
    }
}

/// Opens an obligation after each new fact named `name`, from its stamp
/// with `after`, or for later steps' facts without.
fn oblige_after(cx: &mut CheckCtx<'_, Stamped>, name: Name, after: bool) -> Verdict {
    let view = cx.view();
    let triggers: Vec<u64> = cx
        .new_stamps()
        .iter()
        .copied()
        .filter(|stamp| fact(&view, *stamp) == Some(name))
        .collect();
    for stamp in triggers {
        let after = after.then_some(stamp);
        let obligation = cx.fact_obligation(after, Subject::Owner("w".into()), answer);
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// [`SERVED`]'s check.
fn waits(cx: &mut CheckCtx<'_, Stamped>) -> Verdict {
    oblige_after(cx, Name::Wait, true)
}

/// [`NEXT`]'s check.
fn waits_next(cx: &mut CheckCtx<'_, Stamped>) -> Verdict {
    oblige_after(cx, Name::WaitNext, false)
}

/// [`OBSERVED`]'s check: a fact obligation whose `discharged_by` matches
/// every observation.
fn waits_observed(cx: &mut CheckCtx<'_, Stamped>) -> Verdict {
    let view = cx.view();
    let triggers: Vec<u64> = cx
        .new_stamps()
        .iter()
        .copied()
        .filter(|stamp| fact(&view, *stamp) == Some(Name::WaitObserved))
        .collect();
    for stamp in triggers {
        let obligation = Obligation {
            discharged_by: |_, _| true,
            ..cx.fact_obligation(Some(stamp), Subject::Owner("w".into()), answer)
        };
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// The fact with `stamp`.
fn fact(view: &[(u64, Name)], stamp: u64) -> Option<Name> {
    view.iter()
        .find(|(at, _)| *at == stamp)
        .map(|(_, name)| *name)
}

/// `Done` discharges, `Defer` defers, and anything else is unrelated.
fn answer(_: &ObligationKey, view: &Facts, stamp: u64) -> FactAnswer {
    match fact(view, stamp) {
        Some(Name::Done) => FactAnswer::Discharges,
        Some(Name::Defer(ms)) => {
            FactAnswer::DefersTo(SimInstant::from_start(Duration::from_millis(ms)))
        }
        _ => FactAnswer::Unrelated,
    }
}

/// The trigger history: the view recorded a wait.
fn waited(view: &Facts, _: &ObservationLog) -> bool {
    view.iter()
        .any(|(_, name)| matches!(name, Name::Wait | Name::WaitNext))
}

/// [`waited`], after building the run's first hashed map, which draws its
/// hash seed.
fn drew_and_waited(view: &Facts, log: &ObservationLog) -> bool {
    let seen: HashMap<u64, ()> = view.iter().map(|(stamp, _)| (*stamp, ())).collect();
    !seen.is_empty() && waited(view, log)
}

/// Runs `acts` under a budget of `budget` live steps.
fn run_for(acts: &[Act], budget: u64) -> RunReport {
    run_checked(acts, budget, None)
}

/// [`run_for`], with `extra` after the harness's own checker.
fn run_checked(acts: &[Act], budget: u64, extra: Option<&mut dyn Checker>) -> RunReport {
    let script = Script {
        acts: acts.to_vec(),
        budget,
        drain: 10,
        release_at_end: false,
    };
    match run_harness::<Stamped>(
        &script,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        extra,
    ) {
        Ok(report) => report,
        Err(error) => panic!("the run ends without a harness failure: {error}"),
    }
}

/// Runs `acts` with room to finish.
fn run(acts: &[Act]) -> RunReport {
    run_for(acts, 50)
}

/// Property `id`'s outcome in `report`.
fn outcome(report: &RunReport, id: &str) -> PropertyOutcome {
    report
        .properties
        .iter()
        .find(|p| p.property == id)
        .map(|p| p.outcome.clone())
        .unwrap_or_else(|| panic!("{id} has a report"))
}

/// True if `id` held in `report`.
fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { .. })
}

/// True if `id` was violated in `report`.
fn violated(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Violated { .. })
}

use Act::{Fact, Point, Sleep, Store, Yield};
use Name::{Defer, Done, Wait, WaitNext, WaitObserved};

/// A fact after the trigger's stamp in its own step discharges, and so does
/// one in a later step.
#[test]
fn a_later_fact_discharges() {
    for acts in [
        &[Fact(Wait), Fact(Done)][..],
        &[Fact(Wait), Yield, Fact(Done)],
    ] {
        let report = run(acts);
        assert!(held(&report, SERVED), "{acts:?}");
    }
}

/// A fact before the trigger's stamp never discharges, in the trigger's
/// step or an earlier one, so the obligation expires at quiescence.
#[test]
fn an_earlier_fact_never_discharges() {
    for acts in [
        &[Fact(Done), Fact(Wait)][..],
        &[Fact(Done), Yield, Fact(Wait)],
    ] {
        assert!(violated(&run(acts), SERVED), "{acts:?}");
    }
}

/// One fact discharges only the oldest of two matching obligations, under
/// `Cardinality::One`.
#[test]
fn one_fact_discharges_the_oldest_only() {
    let report = run(&[Fact(Wait), Fact(Wait), Fact(Done)]);
    let served = report
        .properties
        .iter()
        .find(|p| p.property == SERVED)
        .expect("the property reports");
    assert_eq!(served.violations.len(), 1, "{:?}", served.violations);
}

/// With no trigger stamp, a fact in the trigger's own step doesn't count,
/// and one in a later step does.
#[test]
fn an_unstamped_trigger_waits_for_a_later_step() {
    assert!(violated(&run(&[Fact(WaitNext), Fact(Done)]), NEXT));
    assert!(held(&run(&[Fact(WaitNext), Yield, Fact(Done)]), NEXT));
}

/// Observations never discharge a fact obligation, even one whose
/// `discharged_by` matches them, since facts and observations share no
/// order. Later steps' observations leave it open until quiescence.
#[test]
fn observations_never_discharge_a_fact_obligation() {
    assert!(violated(
        &run(&[Fact(WaitObserved), Yield, Yield]),
        OBSERVED
    ));
}

/// A registry rejects a trigger history that reads the view of a property
/// that doesn't need it, since only those get the registry's freshness
/// check.
#[test]
fn a_view_trigger_needs_the_view() {
    let blind = Property {
        needs: &[Source::Schedule],
        ..Stamped::properties().remove(0)
    };
    assert_eq!(
        Registry::new(vec![blind]).err(),
        Some(RegistryError::ViewTriggerWithoutView {
            id: PropertyId::new(SERVED)
        })
    );
}

/// A deferred obligation survives the wait on its timer, and work that
/// runs at the deferred instant discharges it.
#[test]
fn deferred_work_at_its_instant_holds() {
    let report = run(&[Fact(Wait), Fact(Defer(1)), Sleep(1), Fact(Done)]);
    assert!(held(&report, SERVED));
    // Without the deferral, the sleep reaches quiescence first.
    assert!(violated(&run(&[Fact(Wait), Sleep(1), Fact(Done)]), SERVED));
}

/// If the deferred timer is lost, time jumps to the next timer, and that
/// step violates the obligation before its own work counts.
#[test]
fn a_lost_deferred_timer_violates_at_the_next_timer() {
    let report = run(&[Fact(Wait), Fact(Defer(1)), Sleep(60_000), Fact(Done)]);
    let violation = report.end.violation.as_ref().expect("the deadline passed");
    assert_eq!(violation.property, SERVED);
    assert_eq!(
        violation.expected,
        r#"Owner("w") discharged by its deferred deadline, 1000000 ns"#
    );
    assert_eq!(violation.actual, "still open at 60000000000 ns");
    assert_eq!(violation.witness, [1, violation.step]);
}

/// Sol 1, round 4: a deferred deadline doesn't pass while a task is ready
/// with no worker for it. The clock stays, and the run fails as stuck
/// rather than expire the obligation against the node.
#[test]
fn a_deferred_deadline_does_not_pass_while_a_task_is_starved() {
    let script = Script {
        acts: vec![Fact(Wait), Fact(Defer(1)), Act::Starve],
        budget: 50,
        drain: 10,
        release_at_end: false,
    };
    let report = match run_harness::<Stamped>(
        &script,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    ) {
        Ok(report) => report,
        Err(zakura_sim::harness::HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("the harness builds: {error}"),
    };
    assert_eq!(
        report.end.harness,
        Some(zakura_sim::HarnessFailure::StuckWithoutAction {
            status: "worker-starved"
        }),
        "{:?}",
        report.end
    );
    assert_eq!(
        outcome(&report, SERVED),
        PropertyOutcome::not_applicable(NaReason::HarnessFailed)
    );
}

/// Keeps the lifecycle points it's handed.
#[derive(Default)]
struct Points(Vec<Lifecycle>);

impl Checker for Points {
    /// Checks nothing.
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    /// Reports nothing.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }

    /// Keeps each lifecycle point.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.0.extend(events.iter().filter_map(|event| match event {
            CheckEvent::Lifecycle { to, .. } => Some(*to),
            _ => None,
        }));
    }

    /// Nothing: it keeps no obligations.
    fn unfinished(&self) -> zakura_sim::Unfinished {
        zakura_sim::Unfinished::default()
    }
}

/// If the deferred timer is lost and nothing else is armed, the live phase
/// goes idle before the deferred instant. The open obligation sends the run
/// into the drain, whose clock moves just past the deadline, where the
/// obligation expires as a step's finding, and its expiry reaches the
/// checkers after the registry.
#[test]
fn a_deferral_with_nothing_armed_violates_at_its_deadline_in_the_drain() {
    let mut points = Points::default();
    let report = run_checked(&[Fact(Wait), Fact(Defer(1))], 50, Some(&mut points));
    assert_eq!(points.0.last(), Some(&Lifecycle::Expired), "{:?}", points.0);
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert!(report.end.drain.is_some(), "{:?}", report.end);
    assert!(violated(&report, SERVED), "{:?}", report.properties);
    let served = report
        .properties
        .iter()
        .find(|property| property.property == SERVED)
        .expect("the property reports");
    assert_eq!(
        served.violations[0].expected,
        r#"Owner("w") discharged by its deferred deadline, 1000000 ns"#
    );
    assert_eq!(served.violations[0].actual, "still open at 1000001 ns");
    // A step found it, not the run's end.
    assert_eq!(served.end_found, 0, "{served:?}");
}

/// While the task waits on storage, the scheduler alone looks deadlocked,
/// but the storage source still completes the operation. A deferred
/// obligation survives that, and the work after the completion discharges
/// it.
#[test]
fn a_deferral_survives_a_wait_only_a_source_can_end() {
    let report = run(&[Fact(Wait), Fact(Defer(1)), Store, Fact(Done)]);
    assert!(held(&report, SERVED), "{:?}", report.properties);
}

/// A deferral to an instant already behind the clock expires at once, so a
/// later fact of the same step can't discharge it, and a later deferral
/// can't move it.
#[test]
fn an_overdue_deferral_expires_before_later_facts() {
    for acts in [
        &[Sleep(10), Fact(Wait), Fact(Defer(5)), Fact(Done)][..],
        &[
            Sleep(10),
            Fact(Wait),
            Fact(Defer(5)),
            Fact(Defer(20)),
            Sleep(5),
            Fact(Done),
        ],
    ] {
        let report = run(acts);
        let violation = report.end.violation.as_ref().expect("the deadline passed");
        assert_eq!(
            violation.expected, r#"Owner("w") discharged by its deferred deadline, 5000000 ns"#,
            "{acts:?}"
        );
        assert_eq!(violation.actual, "still open at 10000000 ns", "{acts:?}");
    }
}

/// A budget that ends while the obligation is deferred leaves it to the
/// drain, whose clock reaches the task's timer at the deadline, where the
/// discharge is in time.
#[test]
fn the_drain_discharges_an_obligation_deferred_at_the_budget() {
    let report = run_for(&[Fact(Wait), Fact(Defer(1)), Sleep(1), Fact(Done)], 1);
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert!(
        matches!(outcome(&report, SERVED), PropertyOutcome::Held { .. }),
        "{:?}",
        outcome(&report, SERVED)
    );
}

/// A deferred deadline earlier than every timer and boundary is reached
/// first: the drain's clock lands just past it, and the obligation expires
/// there, before the task's later timer could discharge it.
#[test]
fn the_drain_reaches_a_deferred_deadline_before_a_later_timer() {
    let report = run_for(&[Fact(Wait), Fact(Defer(1)), Sleep(5), Fact(Done)], 1);
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    let violation = report.end.violation.as_ref().expect("it violated");
    assert_eq!(violation.property, SERVED);
    assert_eq!(
        report.picks.last().map(|pick| pick.identity.as_str()),
        Some("advance-time:1000001")
    );
}

/// The witness scenario for `acts`.
fn scenario(acts: &[Act]) -> WitnessScenario<Stamped> {
    WitnessScenario {
        scenario: Script {
            acts: acts.to_vec(),
            budget: 50,
            drain: 10,
            release_at_end: false,
        },
        input: Plan::from_fuzz(&[]),
    }
}

/// A trigger history that reads the view passes `Holds` and `Violated`
/// runs that reach it, and fails one that doesn't.
#[test]
fn a_view_trigger_history_is_judged_on_the_final_view() {
    assert_eq!(
        witness(scenario(&[Fact(Wait), Fact(Done)]), SERVED, Expect::Holds),
        Ok(())
    );
    assert_eq!(
        witness(scenario(&[Fact(Wait)]), SERVED, Expect::Violated),
        Ok(())
    );
    assert_eq!(
        witness(scenario(&[Fact(Done)]), SERVED, Expect::Holds),
        Err(WitnessError::LostTriggerHistory)
    );
}

/// A trigger that only the scheduler's shutdown reaches isn't the judged
/// run's. The run is cut with the task stopped at a lock and no drain to
/// resume it, which fails it at the drain's bound, so the wait it would
/// record next never counts, though shutdown resumes it to its poll
/// boundary.
#[test]
fn a_trigger_only_shutdown_reaches_is_lost() {
    let cut = WitnessScenario::<Stamped> {
        scenario: Script {
            acts: vec![Point, Fact(Wait)],
            budget: 1,
            drain: 0,
            release_at_end: false,
        },
        input: Plan::from_fuzz(&[]),
    };
    assert!(
        matches!(
            witness(cut, SERVED, Expect::Holds),
            Err(WitnessError::HarnessEnd(failure)) if failure.starts_with("drain-bound:")
        ),
        "the cut fails at the drain's bound"
    );
}

/// A run that ends at a harness failure fails its witness as `HarnessEnd`
/// without reading the view, which may be unusable then: after stamps that
/// go backward, and after the snapshot is lost.
#[test]
fn a_failed_run_never_reads_its_view() {
    for (how, failure) in [
        (Broken::Stamps, "StampsOutOfOrder"),
        (Broken::Snapshot, "has no snapshot"),
    ] {
        let result = witness(
            scenario(&[Fact(Wait), Yield, Act::Break(how), Yield]),
            SERVED,
            Expect::Holds,
        );
        assert!(
            matches!(&result, Err(WitnessError::HarnessEnd(detail)) if detail.contains(failure)),
            "{how:?}: {result:?}"
        );
    }
}

/// The witness reads the view only once the property's report fits, so a
/// view released at the run's end isn't read for a property that doesn't
/// apply or whose outcome isn't the expected one.
#[test]
fn the_view_is_read_only_for_a_fitting_report() {
    let released = |acts: &[Act]| WitnessScenario::<Stamped> {
        scenario: Script {
            acts: acts.to_vec(),
            budget: 50,
            drain: 10,
            release_at_end: true,
        },
        input: Plan::from_fuzz(&[]),
    };
    assert_eq!(
        witness(
            released(&[Fact(Wait), Fact(Done)]),
            UNDECLARED,
            Expect::Holds
        ),
        Err(WitnessError::Outcome(PropertyOutcome::not_applicable(
            NaReason::Undeclared
        )))
    );
    assert!(matches!(
        witness(
            released(&[Fact(Wait), Fact(Done)]),
            SERVED,
            Expect::Violated
        ),
        Err(WitnessError::Outcome(PropertyOutcome::Held { .. }))
    ));
}

/// A harness failure the trigger history raises, such as a draw outside a
/// step, fails the run, so the witness can't pass.
#[test]
fn a_trigger_that_draws_fails_the_run() {
    let result = witness(scenario(&[Fact(Wait), Fact(Done)]), DRAWING, Expect::Holds);
    assert!(
        matches!(&result, Err(WitnessError::HarnessEnd(detail)) if detail.contains("drew between steps")),
        "{result:?}"
    );
}

/// A report alone can't judge a trigger history that reads the view.
#[test]
fn a_report_alone_cannot_judge_a_view_trigger() {
    let report = run(&[Fact(Wait), Fact(Done)]);
    assert_eq!(
        judge_report::<Stamped>(&report, SERVED, Expect::Holds),
        Err(WitnessError::NoView)
    );
}
