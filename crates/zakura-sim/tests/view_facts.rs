//! hdr-6.0: obligations that view-side facts answer, the deferred deadline,
//! and trigger histories that read the view.
#![cfg(zakura_sim)]

use std::{cell::RefCell, rc::Rc, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_rt::{task, time};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{run_harness, Subsystem},
    property::{
        CheckCtx, FactAnswer, HasView, Kind, LivenessBound, ObligationKey, Property, PropertyId,
        Source, Sources, Status, Subject, TriggerHistory, Verdict,
    },
    witness::{judge_report, witness, Expect, WitnessError, WitnessScenario},
    MemorySink, ObservationLog, Plan, PropertyOutcome, RunConfig, RunReport, Sim,
};

/// A view-side fact the harness's task records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Name {
    /// Opens [`SERVED`]'s obligation after its own stamp.
    Wait,
    /// Opens [`NEXT`]'s obligation, for later steps' facts.
    WaitNext,
    /// Discharges an open obligation.
    Done,
    /// Defers an open obligation to this many milliseconds after the start.
    Defer(u64),
}

/// The facts so far, with their stamps.
type Facts = Vec<(u64, Name)>;

/// One step of the harness's task.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Act {
    /// Records a view-side fact.
    Fact(Name),
    /// Ends the task's poll, so later facts land in a later step.
    Yield,
    /// Sleeps this many milliseconds.
    Sleep(u64),
}

/// The task's acts and the run's live step budget.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Script {
    acts: Vec<Act>,
    budget: u64,
}

/// One task that records stamped facts, as a tap records owner facts.
struct Stamped {
    facts: Rc<RefCell<Facts>>,
}

impl HasView for Stamped {
    type View = Facts;

    /// Every fact so far, with its stamp.
    fn view(&self) -> Self::View {
        self.facts.borrow().clone()
    }

    /// The stamps after `after`.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
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

impl Subsystem for Stamped {
    const NAME: &'static str = "test/view-facts";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Script;
    type Prepared = ();

    fn config(script: &Script) -> RunConfig {
        RunConfig {
            budget: script.budget,
            drain_bound: 10,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Script) {}

    fn build(sim: &mut Sim, script: &Script, (): ()) -> Self {
        let facts = Rc::new(RefCell::new(Vec::new()));
        let task_facts = Rc::clone(&facts);
        let acts = script.acts.clone();
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
                }
            }
        });
        Stamped { facts }
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
        vec![served(SERVED, waits), served(NEXT, waits_next)]
    }

    fn draw(_: &[u8]) -> Script {
        Script {
            acts: Vec::new(),
            budget: 50,
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

/// Runs `acts` under a budget of `budget` live steps.
fn run_for(acts: &[Act], budget: u64) -> RunReport {
    let script = Script {
        acts: acts.to_vec(),
        budget,
    };
    match run_harness::<Stamped>(
        &script,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
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

use Act::{Fact, Sleep, Yield};
use Name::{Defer, Done, Wait, WaitNext};

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

/// A run cut off while the obligation is deferred is inconclusive.
#[test]
fn a_run_cut_off_while_deferred_is_inconclusive() {
    let report = run_for(&[Fact(Wait), Fact(Defer(1)), Sleep(1), Fact(Done)], 1);
    assert_eq!(
        outcome(&report, SERVED),
        PropertyOutcome::Inconclusive {
            reason: "obligation-open".to_owned()
        }
    );
}

/// The witness scenario for `acts`.
fn scenario(acts: &[Act]) -> WitnessScenario<Stamped> {
    WitnessScenario {
        scenario: Script {
            acts: acts.to_vec(),
            budget: 50,
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

/// A report alone can't judge a trigger history that reads the view.
#[test]
fn a_report_alone_cannot_judge_a_view_trigger() {
    let report = run(&[Fact(Wait), Fact(Done)]);
    assert_eq!(
        judge_report::<Stamped>(&report, SERVED, Expect::Holds),
        Err(WitnessError::NoView)
    );
}
