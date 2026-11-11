//! E1: the drain after the budget finishes what the live phase started, so
//! every property ends with an answer (#1840). It offers each source's
//! recovery actions and no adversary act, moves the clock toward every
//! deadline and boundary, and ends dry, which concludes what's open, or at
//! its bound, which is a harness failure.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::Cell,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use support::{advances, config, picks, recorded, run};
use zakura_rt::{sync::CancellationToken, task, time};
use zakura_scheduler::{At, Op, SimInstant, TaskId};
use zakura_sim::{
    check::Phase,
    fake::FakeStorage,
    observe::Exit,
    property::{
        Assume, AssumeRecord, Assumptions, CheckCtx, HasView, Kind, LivenessBound, ObligationKey,
        Pending, Property, PropertyId, Registry, RegistryChecker, Source, Sources, Status, Subject,
        Verdict,
    },
    sim::TaskHold,
    ActionSource, Boundary, Checker, HarnessFailure, Identity, NaReason, Observation,
    ObservationLog, Outcome, Plan, PropertyOutcome, PropertyReport, RecoveryOffer, RunConfig,
    RunEnd, RunReport, Sim, StepCtx, Unfinished, Violation,
};

/// What the checks read besides the log.
#[derive(Default)]
struct World {
    /// Whether the scenario's one assumption holds.
    honest: Cell<bool>,
    /// Whether the harness began to stop the node.
    stopping: Cell<bool>,
    /// The site [`MARKED`] marks pending, if any.
    mark: Cell<Option<Pending>>,
}

impl HasView for World {
    type View = Option<Pending>;

    /// The site to mark, if any.
    fn view(&self) -> Option<Pending> {
        self.mark.get()
    }

    /// Whether the test said the node is stopping.
    fn stopping(&self) -> bool {
        self.stopping.get()
    }
}

/// `request-*` exits open an obligation that a `serve-*` exit discharges.
const SERVED: PropertyId = PropertyId::new("test/served");
/// A late datagram's delivery opens an obligation nothing discharges.
const DELIVERED: PropertyId = PropertyId::new("test/delivered-answered");
/// Marks [`World::mark`]'s site pending at every step.
const MARKED: PropertyId = PropertyId::new("test/marked");

/// Whether `obs` is the completed exit of a task named `prefix*`.
fn exit_of(obs: &Observation, prefix: &str) -> bool {
    matches!(
        obs,
        Observation::TaskExited { task, exit: Exit::Completed } if task.starts_with(prefix)
    )
}

/// Opens an obligation for each `request-*` exit. It applies once one did.
fn requests(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let at: Vec<usize> = cx
        .observations()
        .enumerate()
        .filter(|(_, obs)| exit_of(obs, "request"))
        .map(|(at, _)| at)
        .collect();
    for at in &at {
        let obligation = cx.obligation(Some(*at), Subject::Owner("p1".to_owned()), served);
        cx.oblige(obligation);
    }
    if at.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// [`requests`], but never applying, so a lost assumption is the reason.
fn requests_unapplied(cx: &mut CheckCtx<'_, World>) -> Verdict {
    requests(cx);
    Verdict::NotApplicable
}

/// True for a `serve-*` exit.
fn served(_: &ObligationKey, obs: &Observation) -> bool {
    exit_of(obs, "serve")
}

/// Every request is served within `bound`.
fn served_within(bound: LivenessBound) -> Property<World> {
    Property {
        id: SERVED,
        claim: "every request is served within its bound",
        kind: Kind::Liveness { bound },
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Active,
        check: requests,
        witness: None,
    }
}

/// Opens an obligation at each late delivery, which nothing discharges.
fn deliveries(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let at: Vec<usize> = cx
        .observations()
        .enumerate()
        .filter(|(_, obs)| matches!(obs, Observation::Fake { event, .. } if event == "deliver"))
        .map(|(at, _)| at)
        .collect();
    for at in at {
        let obligation = cx.obligation(Some(at), Subject::Owner("p1".to_owned()), served);
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// Marks the world's site pending while it's set.
fn marks(cx: &mut CheckCtx<'_, World>) -> Verdict {
    if let Some(site) = cx.view() {
        cx.pending(site);
    }
    Verdict::Holds
}

/// A world whose assumption holds.
fn world() -> Rc<World> {
    let world = World::default();
    world.honest.set(true);
    Rc::new(world)
}

/// Runs `properties` under `assumptions` with `config` over what `setup`
/// spawns and adds.
fn checked(
    config: RunConfig,
    world: Rc<World>,
    properties: Vec<Property<World>>,
    assumptions: Assumptions<World>,
    setup: impl FnOnce(&mut Sim),
) -> RunReport {
    let registry = Registry::new(properties).expect("the test's IDs are unique");
    let sources = Sources::observed(&[Source::Schedule, Source::Storage, Source::View])
        .excluding(Source::Frames, "no peer runs");
    let mut checker = RegistryChecker::new(registry, world, assumptions, &sources);
    run(config, Plan::from_fuzz(&[]), setup, &mut checker)
}

/// Property `id`'s outcome in `report`.
fn outcome(report: &RunReport, id: &PropertyId) -> PropertyOutcome {
    report
        .properties
        .iter()
        .find(|p| p.property == id.as_str())
        .map(|p| p.outcome.clone())
        .unwrap_or_else(|| panic!("{id} has a report"))
}

/// The step at which a task named `task` exited.
fn exit_step(report: &RunReport, task: &str) -> u64 {
    report
        .log
        .entries()
        .iter()
        .find(|e| matches!(&e.obs, Observation::TaskExited { task: t, .. } if t == task))
        .map(|e| e.step)
        .unwrap_or_else(|| panic!("{task} exited"))
}

/// The picks the drain made.
fn drain_picks(report: &RunReport) -> Vec<&str> {
    report
        .picks
        .iter()
        .filter(|pick| pick.phase == Phase::Drain)
        .map(|pick| pick.identity.as_str())
        .collect()
}

/// A task that yields `n` times, then finishes.
async fn yields(n: u32) {
    for _ in 0..n {
        task::yield_now().await;
    }
}

/// Seconds as an instant from the start, in nanoseconds.
fn secs(n: u64) -> u128 {
    Duration::from_secs(n).as_nanos()
}

/// A storage service whose completions a hold keeps out of the live
/// enabled set while `held`, as a policy's indefinite hold does. Its
/// recovery offers them anyway.
#[derive(Clone)]
struct Held {
    storage: FakeStorage,
    held: Rc<Cell<bool>>,
}

impl Held {
    /// A held service named `store`, whose operations have an hour to
    /// complete.
    fn new() -> Self {
        Held {
            storage: FakeStorage::new("store", Duration::from_secs(3_600)),
            held: Rc::new(Cell::new(true)),
        }
    }
}

impl ActionSource for Held {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.held.get() {
            self.storage.enabled(now, out);
        }
    }

    /// None while held, since a held completion's deadline would block time.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if !self.held.get() {
            self.storage.boundaries(now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.storage.execute(local, log);
    }

    /// The storage's successes, hold or not.
    fn recovery(&self, now: SimInstant, out: &mut RecoveryOffer) {
        self.storage.recovery(now, out);
    }

    /// Lifts the hold.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {
        self.held.set(false);
    }

    /// None: recovery finishes everything.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Work submitted to a source that nothing will finish.
struct Stranded;

impl ActionSource for Stranded {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("it offers nothing");
    }

    /// Nothing: no action finishes its work.
    fn recovery(&self, _: SimInstant, _: &mut RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _: &mut ObservationLog) {}

    /// The work it strands.
    fn holds_work(&self) -> Option<String> {
        Some("Stranded: the test's operation can never complete".to_owned())
    }
}

/// A datagram whose delivery window opens at `opens`, live and in the
/// drain.
struct Late {
    opens: SimInstant,
    delivered: bool,
}

impl Late {
    /// A datagram whose window opens `after` the start.
    fn new(after: Duration) -> Self {
        Late {
            opens: SimInstant::from_start(after),
            delivered: false,
        }
    }
}

impl ActionSource for Late {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.delivered && now >= self.opens {
            out.push((Identity::deliver("p1->node", 1), 0));
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if !self.delivered && self.opens > now {
            out.push(Boundary::Opens(self.opens));
        }
    }

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        self.delivered = true;
        log.push(Observation::Fake {
            source: "net".to_owned(),
            event: "deliver".to_owned(),
        });
    }

    /// Its delivery once the window opens, and the window as a boundary
    /// until then.
    fn recovery(&self, now: SimInstant, out: &mut RecoveryOffer) {
        self.enabled(now, &mut out.actions);
        if !self.delivered && self.opens > now {
            out.boundaries.push(self.opens);
        }
    }

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _: &mut ObservationLog) {}

    /// None: its window always opens.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// An adversary act that's always enabled live, counting its runs.
struct Attack(Rc<Cell<u32>>);

impl ActionSource for Attack {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        out.push((Identity::peer("p1", "attack"), 0));
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        self.0.set(self.0.get() + 1);
    }

    /// Nothing: an attack finishes no submitted work.
    fn recovery(&self, _: SimInstant, _: &mut RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _: &mut ObservationLog) {}

    /// None: it holds no work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// A control whose `recovered` the drain offers only at a quiet step, as
/// the header recovery control's is, once.
struct Recovering(Rc<Cell<bool>>);

impl ActionSource for Recovering {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        self.0.set(true);
    }

    /// `recovered`, quiet, until it ran.
    fn recovery(&self, _: SimInstant, out: &mut RecoveryOffer) {
        if !self.0.get() {
            out.quiet.push((Identity::peer("ctl", "recovered"), 0));
        }
    }

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _: &mut ObservationLog) {}

    /// None: it holds no work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Holds one task out of every live pick, while anything else is enabled.
struct HoldTask(Rc<Cell<Option<TaskId>>>);

impl TaskHold for HoldTask {
    fn holds(&self, task: TaskId, _: &At) -> bool {
        self.0.get() == Some(task)
    }
}

/// Spawns `request-p1`, which exits at once, opening [`SERVED`]'s
/// obligation, and `serve-p1`, which exits once its operation on `held`
/// completes.
fn request_and_held_serve(sim: &mut Sim, held: &Held) {
    sim.add_source(Box::new(held.clone()));
    sim.scheduler().spawn_root("request-p1", async {});
    let storage = held.storage.clone();
    sim.scheduler().spawn_root("serve-p1", async move {
        storage.submit().await;
    });
}

/// #1840 rule 1: the drain offers recovery actions, here a held storage
/// completion, and never an adversary act, which the live phase offered at
/// every step.
#[test]
fn rule_1_the_drain_offers_recovery_actions_and_no_adversary_act() {
    let attacks = Rc::new(Cell::new(0));
    let held = Held::new();
    let report = checked(
        config(2, 6),
        world(),
        vec![served_within(LivenessBound::BeforeQuiescence)],
        Assumptions::none(),
        |sim| {
            sim.add_source(Box::new(Attack(Rc::clone(&attacks))));
            request_and_held_serve(sim, &held);
            sim.scheduler().spawn_root("busy", yields(20));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    let live = report
        .picks
        .iter()
        .filter(|pick| pick.phase == Phase::Live && pick.identity == "peer:p1:attack")
        .count();
    assert!(live > 0, "the live phase offered the attack");
    assert_eq!(u64::from(attacks.get()), live as u64);
    let drain = drain_picks(&report);
    assert!(
        drain.iter().all(|pick| *pick != "peer:p1:attack"),
        "{drain:?}"
    );
    assert!(drain.contains(&"storage:store:1:success"), "{drain:?}");
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
}

/// The drain completes an operation a live hold kept out of `enabled`, so
/// the task waiting on it finishes.
#[test]
fn the_drain_completes_an_operation_a_hold_kept_from_live() {
    let held = Held::new();
    let report = run(
        config(2, 4),
        Plan::from_fuzz(&[]),
        |sim| {
            request_and_held_serve(sim, &held);
            sim.scheduler().spawn_root("busy", yields(10));
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert!(
        !picks(&report)[..4].contains(&"storage:store:1:success"),
        "the hold kept it out of the live phase"
    );
    assert!(drain_picks(&report).contains(&"storage:store:1:success"));
    let served = exit_step(&report, "serve-p1.1");
    assert!(served > 4, "served in the drain, at step {served}");
}

/// A live end with nothing enabled but a held completion enters the drain,
/// rather than ending idle, and the drain completes it.
#[test]
fn a_live_idle_end_enters_the_drain() {
    let held = Held::new();
    let report = checked(
        config(2, 1_000),
        world(),
        vec![served_within(LivenessBound::VirtualTime(
            Duration::from_secs(1),
        ))],
        Assumptions::none(),
        |sim| request_and_held_serve(sim, &held),
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert!(report.end.steps < 1_000, "the budget never ran out");
    let drain = drain_picks(&report);
    assert_eq!(drain.first(), Some(&"storage:store:1:success"), "{drain:?}");
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
}

/// A task a live hold kept out runs first in the drain, before the clock
/// moves to its obligation's deadline, though a ticker's timers compete.
#[test]
fn a_held_ready_task_runs_before_its_obligation_expires() {
    let serve = Rc::new(Cell::new(None));
    let report = checked(
        config(2, 8),
        world(),
        vec![served_within(LivenessBound::VirtualTime(
            Duration::from_secs(1),
        ))],
        Assumptions::none(),
        |sim| {
            sim.hold_tasks(Box::new(HoldTask(Rc::clone(&serve))));
            sim.scheduler().spawn_root("request-p1", async {});
            let id = sim.scheduler().spawn_root("serve-p1", async {}).task();
            serve.set(Some(id));
            sim.scheduler().spawn_root("ticker", async {
                for _ in 0..30 {
                    time::sleep(Duration::from_millis(100)).await;
                }
            });
        },
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    let served = exit_step(&report, "serve-p1.1");
    assert!(served > 8, "the hold kept it out of the live phase");
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
}

/// A quiet recovery action, such as `recovered`, is offered only at a
/// drain step with no task ready, so a task that changes the evidence runs
/// first.
#[test]
fn a_quiet_recovery_action_waits_for_no_task_ready() {
    let recovered = Rc::new(Cell::new(false));
    let mixed = Rc::new(Cell::new(0));
    let mut checker = zakura_sim::check::after_step({
        let mixed = Rc::clone(&mixed);
        move |cx: &StepCtx<'_>| {
            let task = cx
                .enabled
                .iter()
                .any(|e| matches!(e.action, zakura_sim::Action::RunTask(_)));
            let quiet = cx
                .enabled
                .iter()
                .any(|e| e.identity.to_string() == "peer:ctl:recovered");
            if task && quiet {
                mixed.set(mixed.get() + 1);
            }
            None
        }
    });
    let report = run(
        config(2, 2),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.add_source(Box::new(Recovering(Rc::clone(&recovered))));
            sim.scheduler().spawn_root("busy", yields(10));
        },
        &mut checker,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert!(recovered.get(), "the drain picked it once quiet");
    assert_eq!(mixed.get(), 0, "never beside a ready task");
    assert_eq!(drain_picks(&report).last(), Some(&"peer:ctl:recovered"));
}

/// #1840 rule 2: an obligation opened in the drain, whose deadline falls
/// after every timer, is reached: the clock moves just past it and the
/// obligation expires there.
#[test]
fn rule_2_an_obligation_opened_in_the_drain_expires_at_its_deadline() {
    let held = Held::new();
    let report = checked(
        config(2, 3),
        world(),
        vec![served_within(LivenessBound::VirtualTime(
            Duration::from_secs(1),
        ))],
        Assumptions::none(),
        |sim| {
            sim.add_source(Box::new(held.clone()));
            let storage = held.storage.clone();
            // The request exits only once its held operation completes, in
            // the drain.
            sim.scheduler().spawn_root("request-p1", async move {
                storage.submit().await;
            });
            sim.scheduler().spawn_root("busy", yields(10));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    let request = exit_step(&report, "request-p1.1");
    assert!(request > 3, "the obligation opened in the drain");
    let violation = report.end.violation.as_ref().expect("it violated");
    assert_eq!(violation.property, SERVED.as_str());
    assert_eq!(violation.witness, vec![request, violation.step]);
    assert_eq!(advances(&report), [secs(1) + 1]);
    assert!(report.end.drain.is_some());
}

/// #1840 rule 2: an obligation that nothing left enabled can discharge, or
/// bring to its bound, concludes `Violated` at the dry end, its witness
/// running from its opening to the last step.
#[test]
fn rule_2_a_dry_end_concludes_an_open_obligation() {
    let report = checked(
        config(2, 1_000),
        world(),
        vec![served_within(LivenessBound::Ops {
            task: "ticker.1",
            op: Op::Sleep,
            object: None,
            max: 3,
        })],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("request-p1", async {});
            sim.scheduler().spawn_root("later", yields(3));
        },
    );
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: zakura_sim::IdleStatus::Quiescent
        }
    );
    let request = exit_step(&report, "request-p1.1");
    assert!(
        request < report.end.steps,
        "the run went on after the request"
    );
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Violated {
            witness: vec![request, report.end.steps]
        }
    );
}

/// #1840 rule 2: a drain cut off at its bound with an obligation open fails
/// the run, naming the obligation, never a budget end that leaves it open.
#[test]
fn rule_2_a_drain_cut_off_with_an_obligation_open_names_it() {
    let report = checked(
        RunConfig {
            drain_bound: 20,
            ..config(2, 5)
        },
        world(),
        vec![served_within(LivenessBound::VirtualTime(
            Duration::from_secs(3_600),
        ))],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("request-p1", async {});
            sim.scheduler().spawn_root("spin", async {
                loop {
                    task::yield_now().await;
                }
            });
        },
    );
    assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert_eq!(
        report.end.harness,
        Some(HarnessFailure::DrainBound {
            open: r#"test/served Owner("p1")"#.to_owned()
        })
    );
    assert!(report.end.drain.is_some_and(|drain| drain.exhausted));
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::not_applicable(NaReason::HarnessFailed)
    );
}

/// #1840 rule 4: an assumption lost while an obligation is open withdraws
/// it, so the dry end has nothing to conclude, and a property that never
/// applied reports the loss.
#[test]
fn rule_4_a_lost_assumption_withdraws_what_the_dry_end_would_conclude() {
    let world = world();
    let assumptions = Assumptions::declare(vec![AssumeRecord {
        assume: Assume::HonestPeer,
        params: serde_json::json!({ "peer": "p1" }),
    }])
    .holds_when(|world: &World, _| world.honest.get());
    let report = checked(
        config(2, 1_000),
        Rc::clone(&world),
        vec![Property {
            assumes: &[Assume::HonestPeer],
            check: requests_unapplied,
            ..served_within(LivenessBound::BeforeQuiescence)
        }],
        assumptions,
        |sim| {
            sim.scheduler().spawn_root("request-p1", async {});
            // Lies once the request's obligation is open, while it still
            // keeps the run from quiescence.
            sim.scheduler().spawn_root("liar", async move {
                yields(3).await;
                world.honest.set(false);
                task::yield_now().await;
            });
        },
    );
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::not_applicable(NaReason::AssumptionLost)
    );
}

/// #1840 rule 5: `NotApplicable` always carries its reason, in the end
/// record too.
#[test]
fn rule_5_every_not_applicable_carries_its_reason() {
    let registry =
        Registry::new(vec![served_within(LivenessBound::BeforeQuiescence)]).expect("one property");
    let mut checker = RegistryChecker::new(
        registry,
        world(),
        Assumptions::none(),
        &Sources::observed(&[Source::Schedule]),
    );
    let (report, lines) = recorded(
        config(2, 100),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("idle", async {});
        },
        &mut checker,
    );
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::not_applicable(NaReason::NeverApplied)
    );
    let end = lines.last().expect("the run writes its end record");
    assert!(
        end.contains(
            r#"{"property":"test/served","outcome":{"not-applicable":{"reason":"never-applied"}}}"#
        ),
        "{end}"
    );
}

/// A checker that holds one property and finds a harness failure only at
/// the run's end.
#[derive(Default)]
struct FailsAtEnd {
    failure: Option<HarnessFailure>,
}

impl Checker for FailsAtEnd {
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    /// Reports `test/late` held, and finds missing evidence.
    fn at_end(&mut self, end: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        self.failure = Some(HarnessFailure::MissingEvidence {
            property: "test/late".to_owned(),
            step: end.steps,
            detail: "the end found the evidence short".to_owned(),
        });
        vec![PropertyReport::new(
            "test/late",
            PropertyOutcome::Held { applicable: 1 },
        )]
    }

    /// The failure `at_end` found, once.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        self.failure.take()
    }

    /// Nothing: it keeps no obligations.
    fn unfinished(&self) -> Unfinished {
        Unfinished::default()
    }
}

/// #1840 rule 6: a harness failure is the run's result, not a property's,
/// even one the checker finds only at the end: the run ends `Harness`, and
/// the property it held is `HarnessFailed`.
#[test]
fn rule_6_a_failure_found_only_at_the_end_fails_the_run() {
    let report = run(
        config(2, 100),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("idle", async {});
        },
        &mut FailsAtEnd::default(),
    );
    assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert!(matches!(
        report.end.harness,
        Some(HarnessFailure::MissingEvidence { .. })
    ));
    assert_eq!(
        report.properties[0].outcome,
        PropertyOutcome::not_applicable(NaReason::HarnessFailed)
    );
}

/// A datagram whose window opens late is reached through its recovery
/// boundary, with nothing open, and its delivery opens the run's first
/// obligation, which the drain then brings to its deadline.
#[test]
fn a_late_datagram_opens_the_first_obligation_in_the_drain() {
    let report = checked(
        config(2, 3),
        world(),
        vec![Property {
            id: DELIVERED,
            claim: "a late delivery is answered within a second",
            check: deliveries,
            ..served_within(LivenessBound::VirtualTime(Duration::from_secs(1)))
        }],
        Assumptions::none(),
        |sim| {
            sim.add_source(Box::new(Late::new(Duration::from_secs(5))));
            sim.scheduler().spawn_root("busy", yields(10));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert_eq!(advances(&report), [secs(5), secs(6) + 1]);
    let drain = drain_picks(&report);
    assert!(drain.contains(&"deliver:p1->node:1"), "{drain:?}");
    assert_eq!(
        report.end.violation.as_ref().map(|v| v.property.as_str()),
        Some(DELIVERED.as_str())
    );
}

/// An `Ops` obligation whose task sleeps between its points moves the
/// drain's clock through those timers until it's past its bound.
#[test]
fn an_ops_obligation_whose_task_sleeps_is_reached() {
    let report = checked(
        RunConfig {
            harness_timer_cap: Duration::ZERO,
            ..config(2, 3)
        },
        world(),
        vec![served_within(LivenessBound::Ops {
            task: "ticker.1",
            op: Op::Sleep,
            object: None,
            max: 3,
        })],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("request-p1", async {});
            sim.scheduler().spawn_root("ticker", async {
                loop {
                    time::sleep(Duration::from_secs(1)).await;
                }
            });
        },
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert_eq!(
        report.end.violation.as_ref().map(|v| v.property.as_str()),
        Some(SERVED.as_str())
    );
    assert!(report.end.drain.is_some(), "it expired in the drain");
}

/// A timer due before a recovery boundary gets its step first, so a task
/// it wakes can cancel a later timer before the clock reaches the
/// boundary: the drain lands at 1 s, then at the 10 s window, never at the
/// cancelled 2 s timer.
#[test]
fn an_earlier_timer_cancels_a_later_one_before_a_boundary() {
    let report = run(
        config(2, 2),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.add_source(Box::new(Late::new(Duration::from_secs(10))));
            let token = CancellationToken::new();
            let cancel = token.clone();
            sim.scheduler().spawn_root("canceller", async move {
                time::sleep(Duration::from_secs(1)).await;
                cancel.cancel();
            });
            sim.scheduler().spawn_root("waiter", async move {
                let _ = time::timeout(Duration::from_secs(2), token.cancelled()).await;
            });
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(advances(&report), [secs(1), secs(10)]);
}

/// A periodic timer past the harness timer cap, with nothing open and no
/// boundary, doesn't keep the drain alive: it moves the clock only to the
/// timers due by the cap, then runs dry.
#[test]
fn a_periodic_timer_past_the_cap_does_not_move_the_drain() {
    let report = run(
        RunConfig {
            harness_timer_cap: Duration::from_millis(2_500),
            ..config(2, 1)
        },
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("ticker", async {
                loop {
                    time::sleep(Duration::from_secs(1)).await;
                }
            });
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(advances(&report), [secs(1), secs(2)]);
    assert!(!report.end.drain.expect("it drained").exhausted);
}

/// A bounded harness tail asleep on a timer within the cap, as a redial's
/// 10 s settle is, is reached with nothing open, so the tail finishes in
/// the drain instead of the drain going dry before it.
#[test]
fn a_sleep_within_the_cap_is_reached() {
    let report = run(
        config(2, 1),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("redial", async {
                time::sleep(Duration::from_secs(10)).await;
            });
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(advances(&report), [secs(10)]);
    exit_step(&report, "redial.1");
}

/// A completion the drain can still run keeps it from being quiescent, so
/// a `BeforeQuiescence` obligation waits for it and is discharged.
#[test]
fn a_pending_recovery_completion_discharges_before_quiescence() {
    let held = Held::new();
    let report = checked(
        config(2, 4),
        world(),
        vec![served_within(LivenessBound::BeforeQuiescence)],
        Assumptions::none(),
        |sim| {
            request_and_held_serve(sim, &held);
            sim.scheduler().spawn_root("busy", yields(5));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
}

/// Live, a completion a hold hides isn't eligible, so it doesn't postpone
/// quiescence: the `BeforeQuiescence` obligation expires once the busy
/// task ends, before the run could reach the drain.
#[test]
fn a_live_hold_does_not_postpone_quiescence() {
    let held = Held::new();
    let report = checked(
        config(2, 1_000),
        world(),
        vec![served_within(LivenessBound::BeforeQuiescence)],
        Assumptions::none(),
        |sim| {
            request_and_held_serve(sim, &held);
            sim.scheduler().spawn_root("busy", yields(5));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert_eq!(report.end.drain, None, "it expired live");
    assert!(drain_picks(&report).is_empty());
}

/// Stranded source work with a timed obligation open fails the run as
/// source evidence before the clock moves toward the expiry, so the node
/// isn't blamed for work the harness kept.
#[test]
fn stranded_work_with_a_timed_obligation_open_fails_the_run() {
    let report = checked(
        config(2, 1_000),
        world(),
        vec![served_within(LivenessBound::VirtualTime(
            Duration::from_secs(1),
        ))],
        Assumptions::none(),
        |sim| {
            sim.add_source(Box::new(Stranded));
            sim.scheduler().spawn_root("request-p1", async {});
        },
    );
    assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert_eq!(
        report.end.harness,
        Some(HarnessFailure::SourceEvidence {
            source: "source 0".to_owned(),
            detail: "Stranded: the test's operation can never complete".to_owned(),
        })
    );
    assert!(advances(&report).is_empty(), "the clock never moved");
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::not_applicable(NaReason::HarnessFailed)
    );
}

/// The control: a source that holds work at a dry end, with nothing open,
/// fails the run too.
#[test]
fn a_source_holding_work_at_a_dry_end_fails_the_run() {
    let report = run(
        config(2, 1_000),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.add_source(Box::new(Stranded));
            sim.scheduler().spawn_root("idle", async {});
        },
        &mut zakura_sim::NoChecks,
    );
    assert!(
        matches!(
            &report.end.harness,
            Some(HarnessFailure::SourceEvidence { detail, .. }) if detail.starts_with("Stranded:")
        ),
        "{:?}",
        report.end
    );
}

/// [`MARKED`]'s outcome when the run ends dry with `site` marked, and the
/// node `stopping` or not, with the run's end.
fn marked_at_a_dry_end(site: Pending, stopping: bool) -> (PropertyOutcome, RunEnd) {
    let world = world();
    world.mark.set(Some(site));
    world.stopping.set(stopping);
    let report = checked(
        config(2, 1_000),
        world,
        vec![Property {
            id: MARKED,
            claim: "the mark's site is judged at the dry end",
            kind: Kind::Safety,
            needs: &[Source::Schedule, Source::View],
            check: marks,
            ..served_within(LivenessBound::BeforeQuiescence)
        }],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("idle", async {});
        },
    );
    (outcome(&report, &MARKED), report.end)
}

/// The `Unsettled` outcome at `site`.
fn unsettled(site: Pending) -> PropertyOutcome {
    PropertyOutcome::not_applicable(NaReason::Unsettled { site })
}

/// D4: a fault mark at a dry end isn't convicted generically. With no end
/// judgment of its site's, it's unsettled.
#[test]
fn a_dry_end_leaves_a_fault_mark_unsettled() {
    let (outcome, end) = marked_at_a_dry_end(Pending::FaultOpen, false);
    assert_eq!(end.harness, None);
    assert_eq!(outcome, unsettled(Pending::FaultOpen));
}

/// D4 site 2: an unread boundary at a dry end can never be read, so it
/// fails the run as missing evidence.
#[test]
fn a_dry_end_fails_an_unread_boundary_as_missing_evidence() {
    let (outcome, end) = marked_at_a_dry_end(Pending::UnreadBoundary, false);
    assert!(
        matches!(
            &end.harness,
            Some(HarnessFailure::MissingEvidence { property, .. }) if property == MARKED.as_str()
        ),
        "{end:?}"
    );
    assert_eq!(
        outcome,
        PropertyOutcome::not_applicable(NaReason::HarnessFailed)
    );
}

/// D4: a header teardown mark at a dry end, with no end judgment, is
/// unsettled.
#[test]
fn a_dry_end_leaves_a_header_teardown_mark_unsettled() {
    let (outcome, end) = marked_at_a_dry_end(Pending::TeardownOpen, false);
    assert_eq!(end.harness, None);
    assert_eq!(outcome, unsettled(Pending::TeardownOpen));
}

/// D4: a watch mark at a dry end while the harness stops the node is
/// `NodeStopped`, and unsettled otherwise.
#[test]
fn a_dry_end_reads_a_watch_mark_as_node_stopped() {
    let (outcome, end) = marked_at_a_dry_end(Pending::WatchOpen, true);
    assert_eq!(end.harness, None);
    assert_eq!(
        outcome,
        PropertyOutcome::not_applicable(NaReason::NodeStopped)
    );
    let (outcome, _) = marked_at_a_dry_end(Pending::WatchOpen, false);
    assert_eq!(outcome, unsettled(Pending::WatchOpen));
}

/// D4: a block teardown mark at a dry end is unsettled.
#[test]
fn a_dry_end_leaves_a_block_teardown_mark_unsettled() {
    let (outcome, end) = marked_at_a_dry_end(Pending::BlockTeardownOpen, false);
    assert_eq!(end.harness, None);
    assert_eq!(outcome, unsettled(Pending::BlockTeardownOpen));
}

/// D4: an unjudged departure at a dry end is unsettled.
#[test]
fn a_dry_end_leaves_an_unjudged_departure_unsettled() {
    let (outcome, end) = marked_at_a_dry_end(Pending::UnjudgedDeparture, false);
    assert_eq!(end.harness, None);
    assert_eq!(outcome, unsettled(Pending::UnjudgedDeparture));
}

/// D4: an unread trigger at a dry end is unsettled.
#[test]
fn a_dry_end_leaves_an_unread_trigger_unsettled() {
    let (outcome, end) = marked_at_a_dry_end(Pending::UnreadTrigger, false);
    assert_eq!(end.harness, None);
    assert_eq!(outcome, unsettled(Pending::UnreadTrigger));
}

/// Generators started before the drain are retired at its start: a
/// registered sender stops without closing its link, and one registered
/// after is cancelled at once.
#[test]
fn the_drain_retires_every_generator() {
    let sent = Rc::new(Cell::new(0_u32));
    // Whether the link was up when the sender stopped.
    let link_up = Rc::new(Cell::new(false));
    // Whether the generator registered in the drain started cancelled.
    let late_cancelled = Arc::new(AtomicBool::new(false));
    let report = run(
        config(2, 6),
        Plan::from_fuzz(&[]),
        |sim| {
            let link = CancellationToken::new();
            let token = link.child_token();
            let stop = token.clone();
            let (count, up) = (Rc::clone(&sent), Rc::clone(&link_up));
            sim.spawn_generator("flood", token, async move {
                while !stop.is_cancelled() {
                    count.set(count.get() + 1);
                    task::yield_now().await;
                }
                up.set(!link.is_cancelled());
            });
            let generators = sim.generators();
            let late = Arc::clone(&late_cancelled);
            sim.scheduler().spawn_root("late", async move {
                yields(10).await;
                let token = CancellationToken::new();
                let stop = token.clone();
                generators.spawn("late-flood", token, async move {
                    late.store(stop.is_cancelled(), Ordering::Relaxed);
                });
            });
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert!(sent.get() > 0, "it sent while live");
    assert!(link_up.get(), "the sender stopped with its link up");
    assert!(
        late_cancelled.load(Ordering::Relaxed),
        "a generator registered in the drain is cancelled at once"
    );
}
