//! P1a: the property registry and `RegistryChecker`, over real runs of
//! small tasks (properties.md, P1.1, P1.2 and P1.4), and the P1a
//! follow-up's actual-state contract: source capabilities, view freshness,
//! event positions, discharge cardinality, and retained violations.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
    time::Duration,
};

use support::{config, picks, run};
use zakura_rt::{sync::Mutex, task, time};
use zakura_scheduler::Op;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    fake::FakeStorage,
    model::{Direction, Label, LabelOutcome, LabelledFrame},
    observe::Exit,
    property::{
        Assume, AssumeRecord, Assumptions, Capture, Cardinality, CheckCtx, CheckEvent, HasView,
        Kind, Lifecycle, LivenessBound, Obligation, ObligationKey, Property, PropertyId, Registry,
        RegistryChecker, RegistryError, Source, Sources, Status, Subject, Tee, Verdict,
    },
    ActionSource, Boundary, Checker, HarnessFailure, Identity, IdleStatus, Observation,
    ObservationLog, Outcome, Plan, PropertyOutcome, PropertyReport, RunEnd, RunReport, Sim,
    StepCtx, Violation,
};

/// The state tasks share with the checks.
#[derive(Default)]
struct World {
    /// Whether the scenario's one assumption holds now.
    honest: Cell<bool>,
    /// A counter tasks bump, for safety checks to read.
    level: Cell<u32>,
    /// When the view was captured. `None` reads the state live.
    captured: Cell<Option<Capture>>,
}

impl HasView for World {
    type View = u32;

    /// The counter.
    fn view(&self) -> u32 {
        self.level.get()
    }

    /// The capture the test set, or a live read.
    fn captured(&self) -> Capture {
        self.captured.get().unwrap_or(Capture::Live)
    }
}

/// A world whose assumption holds.
fn honest() -> Rc<World> {
    let world = World::default();
    world.honest.set(true);
    Rc::new(world)
}

/// Whether the world's assumption holds.
fn world_holds(world: &World, _: Assume) -> bool {
    world.honest.get()
}

/// A scenario that declares `HonestPeer`, read from the world.
fn honest_peer() -> Assumptions<World> {
    Assumptions::declare(vec![AssumeRecord {
        assume: Assume::HonestPeer,
        params: serde_json::json!({ "peer": "p1" }),
    }])
    .holds_when(world_holds)
}

const SERVED: PropertyId = PropertyId::new("test/request-served");
const HANDLED: PropertyId = PropertyId::new("test/completion-handled");
const LEVEL: PropertyId = PropertyId::new("test/level-at-most-one");

/// Spawns a root task `label` that sleeps `after`, then exits.
fn exit_after(sim: &mut Sim, label: &'static str, after: Duration) {
    sim.scheduler().spawn_root(label, async move {
        if !after.is_zero() {
            time::sleep(after).await;
        }
    });
}

/// The owner in a completed task's name: `p1` for `request-p1.1`, or for
/// a child such as `serve-p1.1/request-p1.1`, and the prefix `request-`.
fn exited<'o>(obs: &'o Observation, prefix: &str) -> Option<&'o str> {
    match obs {
        Observation::TaskExited {
            task,
            exit: Exit::Completed,
        } => task
            .rsplit('/')
            .next()?
            .strip_prefix(prefix)?
            .split('.')
            .next(),
        _ => None,
    }
}

/// Opens an obligation for each `request-<owner>` task that exits.
fn requests(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let owners: Vec<(usize, String)> = cx
        .observations()
        .enumerate()
        .filter_map(|(at, obs)| Some((at, exited(obs, "request-")?.to_owned())))
        .collect();
    for (at, owner) in &owners {
        let obligation = cx.obligation(Some(*at), Subject::Owner(owner.clone()), served);
        cx.oblige(obligation);
    }
    if owners.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// True if `obs` is the exit of the key's owner's `serve-<owner>` task.
fn served(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(&key.subject, Subject::Owner(owner) if exited(obs, "serve-") == Some(owner))
}

/// Each request is served within `bound`.
fn request_served(
    bound: LivenessBound,
    status: Status,
    assumes: &'static [Assume],
) -> Property<World> {
    Property {
        id: SERVED,
        claim: "every request is served within its bound",
        kind: Kind::Liveness { bound },
        assumes,
        needs: &[Source::Schedule],
        status,
        check: requests,
        witness: None,
    }
}

/// What these runs observe: tasks, fake storage, and the world. They
/// exclude frames, since no peer runs.
fn observed() -> Sources {
    Sources::observed(&[Source::Schedule, Source::Storage, Source::View])
        .excluding(Source::Frames, "no peer runs")
}

/// Runs `properties` under `assumptions` over the tasks `setup` spawns,
/// with `extra` after the registry in a [`Tee`]. Returns the report and the
/// registry checker.
fn run_with<C: Checker>(
    world: Rc<World>,
    properties: Vec<Property<World>>,
    assumptions: Assumptions<World>,
    extra: C,
    setup: impl FnOnce(&mut Sim),
) -> (RunReport, RegistryChecker<World>, C) {
    run_observing(world, properties, assumptions, &observed(), extra, setup)
}

/// [`run_with`], observing `sources`.
fn run_observing<C: Checker>(
    world: Rc<World>,
    properties: Vec<Property<World>>,
    assumptions: Assumptions<World>,
    sources: &Sources,
    extra: C,
    setup: impl FnOnce(&mut Sim),
) -> (RunReport, RegistryChecker<World>, C) {
    let registry = Registry::new(properties).expect("the test's IDs are unique");
    let checker = RegistryChecker::new(registry, world, assumptions, sources);
    let mut tee = Tee(checker, extra);
    let report = run(config(2, 1_000), Plan::from_fuzz(&[]), setup, &mut tee);
    (report, tee.0, tee.1)
}

/// [`run_with`] with no extra checker, returning only the report.
fn report(
    world: Rc<World>,
    properties: Vec<Property<World>>,
    assumptions: Assumptions<World>,
    setup: impl FnOnce(&mut Sim),
) -> RunReport {
    run_with(world, properties, assumptions, Recorder::default(), setup).0
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

/// An `Inconclusive` outcome with `reason`.
fn inconclusive(reason: &str) -> PropertyOutcome {
    PropertyOutcome::Inconclusive {
        reason: reason.to_owned(),
    }
}

/// The step at which `task` exited in `log`.
fn exit_step(log: &ObservationLog, task: &str) -> u64 {
    log.entries()
        .iter()
        .find(|e| matches!(&e.obs, Observation::TaskExited { task: t, .. } if t == task))
        .map(|e| e.step)
        .unwrap_or_else(|| panic!("{task} exited"))
}

/// A checker that records the events it receives and reports one property.
#[derive(Default)]
struct Recorder {
    events: Vec<CheckEvent>,
}

impl Recorder {
    /// The lifecycle points it received, in order.
    fn points(&self) -> Vec<Lifecycle> {
        self.events
            .iter()
            .filter_map(|event| match event {
                CheckEvent::Lifecycle { to, .. } => Some(*to),
                _ => None,
            })
            .collect()
    }

    /// The labelled frames it received, in order.
    fn labelled(&self) -> Vec<LabelledFrame> {
        self.events
            .iter()
            .filter_map(|event| match event {
                CheckEvent::Label { frame, .. } => Some(frame.clone()),
                _ => None,
            })
            .collect()
    }
}

impl Checker for Recorder {
    /// Checks nothing.
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    /// Reports `test/recorder` as not applicable.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        vec![PropertyReport::new(
            "test/recorder",
            PropertyOutcome::NotApplicable,
        )]
    }

    /// Keeps `events`.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.events.extend_from_slice(events);
    }
}

const TWO_SECONDS: LivenessBound = LivenessBound::VirtualTime(Duration::from_secs(2));

/// P1.1: a discharged, an expired, and an open obligation give `Held`,
/// `Violated`, and `Inconclusive`.
#[test]
fn obligations_discharge_expire_and_stay_open() {
    let served_after = |serve: Option<Duration>| {
        report(
            honest(),
            vec![request_served(TWO_SECONDS, Status::Active, &[])],
            Assumptions::none(),
            |sim| {
                exit_after(sim, "request-p1", Duration::ZERO);
                if let Some(serve) = serve {
                    exit_after(sim, "serve-p1", serve);
                }
            },
        )
    };

    let discharged = served_after(Some(Duration::from_secs(1)));
    assert_eq!(
        outcome(&discharged, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
    assert_eq!(
        discharged.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );

    // Time jumps from 0 to the serve's timer at 5 s, past the 2 s deadline.
    let expired = served_after(Some(Duration::from_secs(5)));
    assert_eq!(expired.end.outcome, Outcome::Violation);
    let violation = expired.end.violation.as_ref().expect("it violated");
    assert_eq!(violation.property, SERVED.as_str());
    assert_eq!(
        picks(&expired)[usize::try_from(violation.step - 1).expect("a small step")],
        "advance-time:5000000000",
        "the deadline expires at the advance, before the serve runs"
    );
    let request = exit_step(&expired.log, "request-p1.1");
    assert_eq!(violation.witness, vec![request, violation.step]);
    assert_eq!(
        outcome(&expired, &SERVED),
        PropertyOutcome::Violated {
            witness: violation.witness.clone()
        }
    );

    // The run goes idle at 0 s with the 2 s deadline still ahead.
    let open = served_after(None);
    assert_eq!(outcome(&open, &SERVED), inconclusive("obligation-open"));
}

/// P1.1, review P-03: two owners' obligations and one completion. The
/// completion discharges only its own owner's, and the other expires.
#[test]
fn obligations_match_by_key() {
    let (report, _, recorder) = run_with(
        honest(),
        vec![request_served(TWO_SECONDS, Status::Active, &[])],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "request-p2", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(1));
            exit_after(sim, "serve-p2", Duration::from_secs(5));
        },
    );
    assert_eq!(report.end.outcome, Outcome::Violation);
    let violation = report.end.violation.as_ref().expect("p2 expired");
    assert_eq!(
        violation.witness,
        vec![exit_step(&report.log, "request-p2.1"), violation.step],
        "the expired obligation is p2's"
    );
    assert!(violation.expected.contains("p2"), "{violation:?}");
    assert_eq!(
        recorder.points(),
        [
            Lifecycle::Applicable,
            Lifecycle::Obliged,
            Lifecycle::Obliged,
            Lifecycle::Discharged,
            Lifecycle::Expired,
        ]
    );
}

/// Review P-03: one observation discharges only the oldest obligation it
/// matches, even when two share a key. The two open at 0 s and 1 s, and one
/// serve at 1.5 s discharges the older one. The newer one is left, so it
/// expires when time jumps to 10 s, and the violation names its trigger.
#[test]
fn one_observation_discharges_oldest_only() {
    let (report, _, recorder) = run_with(
        honest(),
        vec![request_served(TWO_SECONDS, Status::Active, &[])],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "request-p1", Duration::from_secs(1));
            exit_after(sim, "serve-p1", Duration::from_millis(1_500));
            exit_after(sim, "tail", Duration::from_secs(10));
        },
    );
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("one obligation expired");
    assert_eq!(
        violation.witness,
        vec![exit_step(&report.log, "request-p1.2"), violation.step],
        "the newer obligation is the one left open"
    );
    let discharges = recorder
        .points()
        .into_iter()
        .filter(|point| *point == Lifecycle::Discharged)
        .count();
    assert_eq!(discharges, 1);
}

/// Opens a `BeforeQuiescence` obligation on the handler when the first
/// storage operation completes.
fn completions(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let completed = cx.observations().position(|obs| {
        matches!(obs, Observation::Fake { source, event }
            if source == "storage" && event.starts_with("store:1:"))
    });
    if completed.is_none() {
        return Verdict::NotApplicable;
    }
    let obligation = cx.obligation(
        completed,
        Subject::Owner("handler".to_owned()),
        handler_exits,
    );
    cx.oblige(obligation);
    Verdict::Holds
}

/// True if `obs` is the handler's exit.
fn handler_exits(_: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::TaskExited { task, .. } if task == "handler.1")
}

/// The handler acts on the first storage completion before quiescence.
fn completion_handled() -> Property<World> {
    Property {
        id: HANDLED,
        claim: "the handler acts on a completion without another external event",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::Storage, Source::Schedule],
        status: Status::Active,
        check: completions,
        witness: None,
    }
}

/// Spawns a handler that waits for `ops` storage operations one at a time,
/// then exits.
fn handler(sim: &mut Sim, ops: usize) {
    let storage = FakeStorage::new("store", Duration::from_secs(1));
    sim.add_source(Box::new(storage.clone()));
    sim.scheduler().spawn_root("handler", async move {
        for _ in 0..ops {
            storage.submit().await;
        }
    });
}

/// P1.1, review P-01: the first completion wakes the handler. The enabled set
/// at that step didn't have the handler in it, but the scheduler's status
/// after the step is `Running`, so the obligation stays open until the
/// handler exits. A handler that waits again instead leaves no task able to
/// run, and the property fails at that step, before the second completion
/// can repair it.
#[test]
fn before_quiescence_reads_post_step_status() {
    let woken = report(
        honest(),
        vec![completion_handled()],
        Assumptions::none(),
        |sim| handler(sim, 1),
    );
    assert_eq!(
        outcome(&woken, &HANDLED),
        PropertyOutcome::Held { applicable: 1 }
    );

    let parked = report(
        honest(),
        vec![completion_handled()],
        Assumptions::none(),
        |sim| handler(sim, 2),
    );
    assert_eq!(parked.end.outcome, Outcome::Violation);
    let violation = parked.end.violation.as_ref().expect("it violated");
    let picks = picks(&parked);
    let step = usize::try_from(violation.step).expect("a small step");
    assert!(
        picks[step - 1].starts_with("task:handler.1:"),
        "the violation is at the handler's step: {picks:?}"
    );
    assert!(
        picks[step - 2].starts_with("storage:store:1:"),
        "right after the completion that woke it: {picks:?}"
    );
    assert_eq!(picks.len(), step, "the run ended at that step");
    assert!(
        !picks
            .iter()
            .any(|pick| pick.starts_with("storage:store:2:")),
        "the second completion never ran: {picks:?}"
    );
}

/// P1.2: a property whose assumptions the scenario doesn't declare is
/// `NotApplicable`, and its check never runs.
#[test]
fn undeclared_assumption_not_applicable() {
    let report = report(
        honest(),
        vec![
            request_served(TWO_SECONDS, Status::Active, &[Assume::HonestPeer]),
            completion_handled(),
        ],
        Assumptions::none(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(1));
            handler(sim, 1);
        },
    );
    assert_eq!(outcome(&report, &SERVED), PropertyOutcome::NotApplicable);
    assert_eq!(
        outcome(&report, &HANDLED),
        PropertyOutcome::Held { applicable: 1 },
        "a property with no assumptions still runs"
    );
}

/// P1.2: a property whose declared assumptions never hold is
/// `Inconclusive`.
#[test]
fn assumptions_never_held_inconclusive() {
    let world = Rc::new(World::default());
    let report = report(
        world,
        vec![request_served(
            TWO_SECONDS,
            Status::Active,
            &[Assume::HonestPeer],
        )],
        honest_peer(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(1));
        },
    );
    assert_eq!(
        outcome(&report, &SERVED),
        inconclusive("assumptions-never-held")
    );
}

/// P1.2, review P-03: an obligation opened long after `holds_since` gets its
/// full interval, counted from its trigger.
#[test]
fn deadline_starts_at_trigger() {
    let (report, checker, _) = run_with(
        honest(),
        vec![request_served(
            TWO_SECONDS,
            Status::Active,
            &[Assume::HonestPeer],
        )],
        honest_peer(),
        Recorder::default(),
        |sim| {
            exit_after(sim, "request-p1", Duration::from_secs(10));
            exit_after(sim, "serve-p1", Duration::from_secs(11));
        },
    );
    assert_eq!(checker.holds_since(&SERVED), Some(1));
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Held { applicable: 1 },
        "served 1 s after a request at 10 s, though 11 s after holds_since"
    );
}

/// P1.2, review P-03: an assumption that breaks and returns ends the open
/// obligation `Inconclusive`, and `holds_since` restarts when it returns.
#[test]
fn lost_assumption_inconclusive() {
    let world = honest();
    let fault = world.clone();
    let (report, checker, _) = run_with(
        world,
        vec![request_served(
            LivenessBound::VirtualTime(Duration::from_secs(10)),
            Status::Active,
            &[Assume::HonestPeer],
        )],
        honest_peer(),
        Recorder::default(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(3));
            sim.scheduler().spawn_root("fault", async move {
                time::sleep(Duration::from_secs(1)).await;
                fault.honest.set(false);
                time::sleep(Duration::from_secs(1)).await;
                fault.honest.set(true);
            });
        },
    );
    assert_eq!(outcome(&report, &SERVED), inconclusive("assumption-lost"));
    assert_eq!(
        checker.holds_since(&SERVED),
        Some(exit_step(&report.log, "fault.1")),
        "holds_since restarts at the step the fault ends"
    );
}

/// Bumps the counter once.
fn bump(world: &World) {
    world.level.set(world.level.get() + 1);
}

/// Fails once the counter passes 1.
fn level_at_most_one(cx: &mut CheckCtx<'_, World>) -> Verdict {
    match cx.view() {
        level if level > 1 => cx.violated("level at most 1", format!("level {level}")),
        _ => Verdict::Holds,
    }
}

/// A safety property over the view, with `status`.
fn level(status: Status) -> Property<World> {
    Property {
        id: LEVEL,
        claim: "the counter never passes 1",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status,
        check: level_at_most_one,
        witness: None,
    }
}

/// M6.1: a `Known` violation is recorded, the run goes on to its end, and
/// the property reports `Violated`. The same violation of an `Active`
/// property ends the run.
#[test]
fn known_violation_does_not_stop_the_run() {
    let bumps = |status| {
        let world = honest();
        let task_world = world.clone();
        report(world, vec![level(status)], Assumptions::none(), |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
                task::yield_now().await;
                bump(&task_world);
            });
            exit_after(sim, "tail", Duration::from_secs(1));
        })
    };

    let known = bumps(Status::Known { issue: 1523 });
    assert_eq!(
        known.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    let second_bump = exit_step(&known.log, "bump.1");
    assert!(exit_step(&known.log, "tail.1") > second_bump);
    assert_eq!(
        outcome(&known, &LEVEL),
        PropertyOutcome::Violated {
            witness: vec![second_bump]
        }
    );

    let active = bumps(Status::Active);
    assert_eq!(active.end.outcome, Outcome::Violation);
    assert_eq!(
        active.end.violation.map(|v| v.step),
        Some(exit_step(&active.log, "bump.1"))
    );
}

/// An `Ops` bound counts the named task's points of one operation after
/// the obligation opens.
#[test]
fn ops_bound_counts_task_points() {
    let spins = |max| {
        let property = request_served(
            LivenessBound::Ops {
                task: "spinner.1",
                op: Op::Lock,
                object: None,
                max,
            },
            Status::Active,
            &[],
        );
        report(honest(), vec![property], Assumptions::none(), |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(1));
            // Starts after the request, so every lock counts.
            sim.scheduler().spawn_root("spinner", async {
                time::sleep(Duration::from_millis(1)).await;
                let lock = Mutex::new(());
                for _ in 0..4 {
                    drop(lock.lock().expect("no task panics holding it"));
                }
            });
        })
    };
    assert_eq!(
        outcome(&spins(4), &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
    let over = spins(3);
    assert_eq!(over.end.outcome, Outcome::Violation);
    let violation = over.end.violation.expect("the fourth lock expired it");
    assert_eq!(violation.actual, "still open after 4");
}

/// [`Tee`] returns the first checker's violation and concatenates both
/// checkers' reports.
#[test]
fn tee_runs_both_checkers() {
    let (report, _, _) = run_with(
        honest(),
        vec![request_served(TWO_SECONDS, Status::Active, &[])],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            exit_after(sim, "request-p1", Duration::ZERO);
            exit_after(sim, "serve-p1", Duration::from_secs(5));
        },
    );
    assert_eq!(
        report.end.violation.map(|v| v.property).as_deref(),
        Some(SERVED.as_str())
    );
    let ids: Vec<&str> = report
        .properties
        .iter()
        .map(|p| p.property.as_str())
        .collect();
    assert_eq!(ids, [SERVED.as_str(), "test/recorder"]);
}

/// P1.4: `Registry::new` rejects two properties with one ID.
#[test]
fn duplicate_id_rejected() {
    let result = Registry::new(vec![level(Status::Active), level(Status::Draft)]);
    assert_eq!(result.err(), Some(RegistryError::DuplicateId { id: LEVEL }));
}

/// `Registry::new` rejects an `Ops` bound that names an object until
/// zakura#1643 keeps `Point.object`.
#[test]
fn ops_bound_with_object_rejected() {
    let property = request_served(
        LivenessBound::Ops {
            task: "reactor.1",
            op: Op::Recv,
            object: Some("wire"),
            max: 1,
        },
        Status::Active,
        &[],
    );
    assert_eq!(
        Registry::new(vec![property]).err(),
        Some(RegistryError::OpsObject { id: SERVED })
    );
}

/// Opens a `BeforeQuiescence` obligation for each `request-<owner>` task
/// that a step spawns.
fn spawned_requests(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let owners: Vec<(usize, String)> = cx
        .observations()
        .enumerate()
        .filter_map(|(at, obs)| match obs {
            Observation::TaskSpawned { task } => {
                let name = task.rsplit('/').next()?.strip_prefix("request-")?;
                Some((at, name.split('.').next()?.to_owned()))
            }
            _ => None,
        })
        .collect();
    for (at, owner) in &owners {
        let obligation = cx.obligation(Some(*at), Subject::Owner(owner.clone()), served);
        cx.oblige(obligation);
    }
    if owners.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// Review: a step that logs `serve-p1`'s exit and then the spawn of
/// `request-p1` mustn't let the earlier exit discharge the later request.
/// The request is never served, so the property fails at quiescence.
#[test]
fn earlier_observation_does_not_discharge_later_trigger() {
    let property = Property {
        id: SERVED,
        claim: "every spawned request is served before quiescence",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Active,
        check: spawned_requests,
        witness: None,
    };
    let report = report(honest(), vec![property], Assumptions::none(), |sim| {
        sim.scheduler().spawn_root("serve-p1", async {
            task::spawn_named("request-p1", async {});
        });
    });
    let trigger = report
        .log
        .entries()
        .iter()
        .position(|e| matches!(&e.obs, Observation::TaskSpawned { task } if task.ends_with("/request-p1.1")))
        .expect("the request was spawned");
    let entries = report.log.entries();
    assert_eq!(
        entries[trigger - 1],
        zakura_sim::observe::Entry {
            step: entries[trigger].step,
            obs: Observation::TaskExited {
                task: "serve-p1.1".to_owned(),
                exit: Exit::Completed
            }
        },
        "the serve's exit comes first in the request's step"
    );
    assert!(
        matches!(outcome(&report, &SERVED), PropertyOutcome::Violated { .. }),
        "{:?}",
        report.properties
    );
}

/// Opens an obligation, discharged by the spinner's second lock, for each
/// `request-<owner>` task that exits.
fn requests_until_second_lock(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let triggers: Vec<usize> = cx
        .observations()
        .enumerate()
        .filter(|(_, obs)| exited(obs, "request-").is_some())
        .map(|(at, _)| at)
        .collect();
    for at in &triggers {
        let obligation = cx.obligation(Some(*at), Subject::Owner("p1".to_owned()), second_lock);
        cx.oblige(obligation);
    }
    if triggers.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// True if `obs` is the spinner reaching its second lock.
fn second_lock(_: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::TaskReached { task, at, .. } if task == "spinner.1" && at == "lock#2")
}

/// Review: with `max: 1`, the spinner's second lock is one point too many.
/// That it also matches `discharged_by` mustn't save the obligation.
#[test]
fn ops_bound_counts_before_discharge() {
    let property = Property {
        id: SERVED,
        claim: "a request is served within one spinner lock",
        kind: Kind::Liveness {
            bound: LivenessBound::Ops {
                task: "spinner.1",
                op: Op::Lock,
                object: None,
                max: 1,
            },
        },
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Active,
        check: requests_until_second_lock,
        witness: None,
    };
    let report = report(honest(), vec![property], Assumptions::none(), |sim| {
        exit_after(sim, "request-p1", Duration::ZERO);
        sim.scheduler().spawn_root("spinner", async {
            time::sleep(Duration::from_millis(1)).await;
            let lock = Mutex::new(());
            for _ in 0..2 {
                drop(lock.lock().expect("no task panics holding it"));
            }
        });
    });
    assert_eq!(
        report.end.outcome,
        Outcome::Violation,
        "{:?}",
        report.properties
    );
    let violation = report.end.violation.expect("the second lock expired it");
    assert_eq!(violation.actual, "still open after 2");
}

/// Review: a poll boundary logs `TaskWaiting`, never a `poll` point, so an
/// `Ops` bound on `Op::Poll` would never count. `Registry::new` rejects it.
#[test]
fn ops_bound_on_poll_rejected() {
    let property = request_served(
        LivenessBound::Ops {
            task: "reactor.1",
            op: Op::Poll,
            object: None,
            max: 1,
        },
        Status::Active,
        &[],
    );
    assert_eq!(
        Registry::new(vec![property]).err(),
        Some(RegistryError::OpsPoll { id: SERVED })
    );
}

/// Applies only once the counter passes 1, and then fails.
fn level_violated_when_applicable(cx: &mut CheckCtx<'_, World>) -> Verdict {
    match cx.view() {
        level if level > 1 => cx.violated("level at most 1", format!("level {level}")),
        _ => Verdict::NotApplicable,
    }
}

/// Review: a check that goes straight from `NotApplicable` to `Violated`
/// still applied, so the registry emits `Lifecycle::Applicable` for it.
#[test]
fn first_violation_emits_applicable() {
    let world = honest();
    let task_world = world.clone();
    let property = Property {
        check: level_violated_when_applicable,
        ..level(Status::Known { issue: 1523 })
    };
    let (report, _, recorder) = run_with(
        world,
        vec![property],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
                task::yield_now().await;
                bump(&task_world);
            });
        },
    );
    assert!(matches!(
        outcome(&report, &LEVEL),
        PropertyOutcome::Violated { .. }
    ));
    assert_eq!(recorder.points(), [Lifecycle::Applicable]);
}

/// Opens an obligation, discharged by the next timer to fire, for each
/// `request-<owner>` task that exits.
fn requests_until_timer(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let triggers: Vec<usize> = cx
        .observations()
        .enumerate()
        .filter(|(_, obs)| exited(obs, "request-").is_some())
        .map(|(at, _)| at)
        .collect();
    for at in &triggers {
        let obligation = cx.obligation(Some(*at), Subject::Owner("p1".to_owned()), timer_fired);
        cx.oblige(obligation);
    }
    if triggers.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// True if `obs` is a time advance that fired a timer.
fn timer_fired(_: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::TimeAdvanced { fired, .. } if *fired > 0)
}

/// A request at 0 s, discharged by the first timer, which fires at `timer`,
/// under a 2 s bound.
fn timer_within_two_seconds(timer: Duration) -> RunReport {
    let property = Property {
        check: requests_until_timer,
        ..request_served(TWO_SECONDS, Status::Active, &[])
    };
    report(honest(), vec![property], Assumptions::none(), |sim| {
        exit_after(sim, "request-p1", Duration::ZERO);
        exit_after(sim, "tail", timer);
    })
}

/// Review: the advance to 5 s matches `discharged_by`, but it comes after the
/// 2 s deadline, so the obligation expires before that advance can
/// discharge it.
#[test]
fn virtual_time_deadline_checked_before_discharge() {
    let late = timer_within_two_seconds(Duration::from_secs(5));
    assert_eq!(
        late.end.outcome,
        Outcome::Violation,
        "{:?}",
        late.properties
    );
    let violation = late.end.violation.expect("the deadline passed");
    assert_eq!(violation.actual, "still open at 5000000000 ns");
}

/// A discharge at exactly the deadline is in time.
#[test]
fn virtual_time_discharge_at_deadline_holds() {
    let on_time = timer_within_two_seconds(Duration::from_secs(2));
    assert_eq!(
        outcome(&on_time, &SERVED),
        PropertyOutcome::Held { applicable: 1 }
    );
}

const NO_BAD_FRAMES: PropertyId = PropertyId::new("test/no-bad-frames");

/// Finds no offending frame, since it looks for frames among observations
/// that never include any.
fn no_bad_frames(_: &mut CheckCtx<'_, World>) -> Verdict {
    Verdict::Holds
}

/// A safety property that reads frames.
fn frames_property() -> Property<World> {
    Property {
        id: NO_BAD_FRAMES,
        claim: "no peer sends an offending frame",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Frames],
        status: Status::Active,
        check: no_bad_frames,
        witness: None,
    }
}

/// Review: a run that observes only scheduling never saw a frame, so a
/// property that needs frames is `NotApplicable`, not `Held` for finding no
/// bad ones. Its report names the exclusion, and it never runs, so it never
/// emits `Applicable`.
#[test]
fn excluded_source_not_applicable() {
    let (report, checker, recorder) = run_with(
        honest(),
        vec![frames_property()],
        Assumptions::none(),
        Recorder::default(),
        |sim| exit_after(sim, "request-p1", Duration::ZERO),
    );
    assert_eq!(
        outcome(&report, &NO_BAD_FRAMES),
        PropertyOutcome::NotApplicable
    );
    assert_eq!(recorder.points(), []);
    assert_eq!(
        checker.exclusion(&NO_BAD_FRAMES),
        Some("Frames: no peer runs")
    );
    let excluded = report
        .properties
        .iter()
        .find(|p| p.property == NO_BAD_FRAMES.as_str())
        .and_then(|p| p.excluded.as_deref());
    assert_eq!(excluded, Some("Frames: no peer runs"));

    // A harness that observes frames runs it.
    let (report, checker, _) = run_observing(
        honest(),
        vec![frames_property()],
        Assumptions::none(),
        &Sources::observed(&[Source::Frames, Source::Schedule]),
        Recorder::default(),
        |sim| exit_after(sim, "request-p1", Duration::ZERO),
    );
    assert_eq!(
        outcome(&report, &NO_BAD_FRAMES),
        PropertyOutcome::Held { applicable: 1 }
    );
    assert_eq!(checker.exclusion(&NO_BAD_FRAMES), None);
}

/// P1 follow-up: a source a property needs that the harness neither
/// observes nor excludes is a harness failure, never `Held` or a silent
/// `NotApplicable`.
#[test]
fn missing_source_fails_the_harness() {
    let (report, _, _) = run_observing(
        honest(),
        vec![frames_property()],
        Assumptions::none(),
        &Sources::observed(&[Source::Schedule]),
        Recorder::default(),
        |sim| exit_after(sim, "request-p1", Duration::ZERO),
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(
        report.end.harness,
        Some(HarnessFailure::MissingSource {
            property: NO_BAD_FRAMES.to_string(),
            source: "Frames".to_owned(),
        })
    );
    assert_eq!(report.end.steps, 0, "the run fails before its first step");
}

/// P1 follow-up: a check that reads the view must see the state at the
/// step's boundary. A snapshot from an earlier boundary, or none, fails the
/// run as a harness failure before the check runs.
#[test]
fn stale_view_fails_the_harness() {
    for (captured, shown) in [(Capture::AfterStep(0), Some(0)), (Capture::Missing, None)] {
        let world = honest();
        world.captured.set(Some(captured));
        let report = report(
            world,
            vec![level(Status::Active)],
            Assumptions::none(),
            |sim| {
                exit_after(sim, "tail", Duration::ZERO);
            },
        );
        assert_eq!(report.end.outcome, Outcome::Harness);
        assert_eq!(
            report.end.harness,
            Some(HarnessFailure::StaleView {
                property: LEVEL.to_string(),
                step: 1,
                captured: shown,
            })
        );
    }
}

const EVEN: PropertyId = PropertyId::new("test/counter-even");

/// Writes `value` to the counter only if it is even: the guarded path.
fn guarded_write(world: &World, value: u32) {
    if value.is_multiple_of(2) {
        world.level.set(value);
    }
}

/// Fails while the counter is odd, reading only the actual counter.
fn counter_even(cx: &mut CheckCtx<'_, World>) -> Verdict {
    match cx.view() {
        level if level % 2 == 1 => cx.violated("an even counter", format!("counter {level}")),
        _ => Verdict::Holds,
    }
}

/// A safety property over the actual counter.
fn even() -> Property<World> {
    Property {
        id: EVEN,
        claim: "the counter stays even",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: counter_even,
        witness: None,
    }
}

/// P1 follow-up acceptance: a guarded writer `a` and an unguarded writer
/// `d` both write the counter. The invariant reads the counter itself, so it
/// catches an odd value from either path, with no guard event in the log.
#[test]
fn invariant_reads_actual_state_from_any_writer() {
    let run = |unguarded: Option<u32>| {
        let world = honest();
        let a = world.clone();
        let d = world.clone();
        report(world, vec![even()], Assumptions::none(), |sim| {
            sim.scheduler().spawn_root("a", async move {
                guarded_write(&a, 3);
                task::yield_now().await;
                guarded_write(&a, 4);
            });
            sim.scheduler().spawn_root("d", async move {
                task::yield_now().await;
                if let Some(value) = unguarded {
                    d.level.set(value);
                }
            });
        })
    };

    let guarded = run(None);
    assert!(
        matches!(outcome(&guarded, &EVEN), PropertyOutcome::Held { .. }),
        "the guard refused 3: {:?}",
        guarded.properties
    );

    let bypassed = run(Some(5));
    let violation = bypassed.end.violation.as_ref().expect("d wrote 5");
    assert_eq!(violation.step, exit_step(&bypassed.log, "d.1"));
    assert_eq!(violation.actual, "counter 5");
    assert!(
        !bypassed
            .log
            .entries()
            .iter()
            .any(|e| matches!(e.obs, Observation::Fake { .. })),
        "nothing but the counter shows the bad write"
    );
}

/// P1 follow-up acceptance: the registry reads the view at each step's
/// boundary. A task that writes 1 and restores 0 with no preemption point
/// between them leaves no trace there, so the check can't see it. That is
/// the documented scope of a boundary check, not coverage of the
/// intermediate state. A preemption point between the two writes is a
/// boundary, and the check sees the write there.
#[test]
fn write_then_restore_is_seen_only_at_a_boundary() {
    let run = |yield_between: bool| {
        let world = honest();
        let task_world = world.clone();
        report(world, vec![even()], Assumptions::none(), |sim| {
            sim.scheduler().spawn_root("writer", async move {
                task_world.level.set(1);
                if yield_between {
                    task::yield_now().await;
                }
                task_world.level.set(0);
            });
        })
    };

    let inside_one_step = run(false);
    assert_eq!(
        outcome(&inside_one_step, &EVEN),
        PropertyOutcome::Held { applicable: 1 },
        "the restore lands before the step's boundary"
    );

    let across_a_boundary = run(true);
    assert_eq!(across_a_boundary.end.outcome, Outcome::Violation);
    assert_eq!(
        across_a_boundary.end.violation.map(|v| v.actual).as_deref(),
        Some("counter 1")
    );
}

/// A scripted wire for tests: each action logs one step's events, in order,
/// as `wire` fake events. Time can't pass while steps remain.
#[derive(Clone)]
struct Wire(Rc<RefCell<VecDeque<&'static [&'static str]>>>);

impl Wire {
    /// A wire that takes one action per entry of `steps`.
    fn new(steps: &[&'static [&'static str]]) -> Self {
        Wire(Rc::new(RefCell::new(steps.iter().copied().collect())))
    }
}

impl ActionSource for Wire {
    /// The next step, while any remain.
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let left = self.0.borrow().len();
        if left > 0 {
            out.push((Identity::peer("wire", &left.to_string()), 0));
        }
    }

    /// Holds time at now while steps remain.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if !self.0.borrow().is_empty() {
            out.push(Boundary::Deadline(now));
        }
    }

    /// Logs the next step's events.
    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let events = self
            .0
            .borrow_mut()
            .pop_front()
            .expect("`enabled` offered the next step");
        for event in events {
            log.push(Observation::Fake {
                source: "wire".to_owned(),
                event: (*event).to_owned(),
            });
        }
    }
}

/// The subject of a wire event `kind:<subject>`.
fn wire<'o>(obs: &'o Observation, kind: &str) -> Option<&'o str> {
    match obs {
        Observation::Fake { source, event } if source == "wire" => {
            event.strip_prefix(kind)?.strip_prefix(':')
        }
        _ => None,
    }
}

/// Opens one obligation per `kind:<subject>` wire event this step, with
/// `subject` building its subject and `discharged_by` and `cardinality` set.
fn oblige_each(
    cx: &mut CheckCtx<'_, World>,
    kind: &str,
    subject: fn(String) -> Subject,
    discharged_by: fn(&ObligationKey, &Observation) -> bool,
    cardinality: Cardinality,
) -> Verdict {
    let triggers: Vec<(usize, String)> = cx
        .observations()
        .enumerate()
        .filter_map(|(at, obs)| Some((at, wire(obs, kind)?.to_owned())))
        .collect();
    for (at, name) in &triggers {
        let obligation = Obligation {
            cardinality,
            ..cx.obligation(Some(*at), subject(name.clone()), discharged_by)
        };
        cx.oblige(obligation);
    }
    if triggers.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// Opens an owner's obligation for each `req:<owner>`.
fn wire_requests(cx: &mut CheckCtx<'_, World>) -> Verdict {
    oblige_each(cx, "req", Subject::Owner, wire_done, Cardinality::One)
}

/// True if `obs` is `done:<owner>` for the key's owner.
fn wire_done(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(&key.subject, Subject::Owner(owner) if wire(obs, "done") == Some(owner))
}

/// Opens a session's consequence obligation for each `fault:<session>`.
fn wire_faults(cx: &mut CheckCtx<'_, World>) -> Verdict {
    oblige_each(
        cx,
        "fault",
        Subject::Session,
        wire_close,
        Cardinality::Every,
    )
}

/// True if `obs` is `close:<session>` for the key's session.
fn wire_close(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(&key.subject, Subject::Session(session) if wire(obs, "close") == Some(session))
}

/// A two-second liveness property over the wire with `check`.
fn wire_property(check: fn(&mut CheckCtx<'_, World>) -> Verdict) -> Property<World> {
    Property {
        id: SERVED,
        claim: "the node answers each wire trigger within 2 s",
        kind: Kind::Liveness { bound: TWO_SECONDS },
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Known { issue: 1523 },
        check,
        witness: None,
    }
}

/// Runs `check` over the wire `steps`, then lets time reach 5 s, past every
/// deadline.
fn over_wire(
    check: fn(&mut CheckCtx<'_, World>) -> Verdict,
    steps: &[&'static [&'static str]],
) -> (RunReport, Recorder) {
    let wire = Wire::new(steps);
    let (report, _, recorder) = run_with(
        honest(),
        vec![wire_property(check)],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            sim.add_source(Box::new(wire));
            exit_after(sim, "tail", Duration::from_secs(5));
        },
    );
    (report, recorder)
}

/// Property `id`'s report in `report`.
fn property_report<'r>(report: &'r RunReport, id: &PropertyId) -> &'r PropertyReport {
    report
        .properties
        .iter()
        .find(|p| p.property == id.as_str())
        .unwrap_or_else(|| panic!("{id} has a report"))
}

/// P1 follow-up acceptance: three events in one step keep their order. The
/// completion between two requests from one owner discharges the first, and
/// the second, which came after it, stays open and expires.
#[test]
fn events_in_one_step_keep_their_order() {
    let (report, recorder) = over_wire(wire_requests, &[&["req:p1", "done:p1", "req:p1"]]);
    assert_eq!(
        recorder.points(),
        [
            Lifecycle::Applicable,
            Lifecycle::Obliged,
            Lifecycle::Obliged,
            Lifecycle::Discharged,
            Lifecycle::Expired,
        ]
    );
    let violations = &property_report(&report, &SERVED).violations;
    assert_eq!(violations.len(), 1, "{violations:?}");
}

/// P1 follow-up acceptance: one close discharges every earlier consequence
/// on its session, and only those. `s2`'s consequence waits on another
/// session, and `s3`'s close came before its trigger in the same step, so
/// both expire. One step expires both, and the report keeps both
/// violations.
#[test]
fn close_discharges_every_earlier_consequence() {
    let (report, recorder) = over_wire(
        wire_faults,
        &[
            &["fault:s1"],
            &["fault:s1", "fault:s2"],
            &["close:s3", "fault:s3"],
            &["close:s1"],
        ],
    );
    let points = recorder.points();
    let count = |point| points.iter().filter(|p| **p == point).count();
    assert_eq!(count(Lifecycle::Obliged), 4);
    assert_eq!(count(Lifecycle::Discharged), 2, "one close, two discharges");
    assert_eq!(count(Lifecycle::Expired), 2);

    let violations = &property_report(&report, &SERVED).violations;
    assert_eq!(violations.len(), 2, "{violations:?}");
    assert_eq!(violations[0].step, violations[1].step);
    assert!(violations[0].expected.contains("s2"), "{violations:?}");
    assert!(violations[1].expected.contains("s3"), "{violations:?}");
    assert_eq!(
        outcome(&report, &SERVED),
        PropertyOutcome::Violated {
            witness: violations[0].witness.clone()
        }
    );
}

/// A counter-is-zero property with `id` and `status`.
fn level_zero(id: PropertyId, status: Status) -> Property<World> {
    Property {
        id,
        check: |cx| match cx.view() {
            0 => Verdict::Holds,
            level => cx.violated("level 0", format!("level {level}")),
        },
        ..level(status)
    }
}

/// P1 follow-up acceptance: the registry evaluates every property at the
/// failing boundary before it ends the run. A `Known` property before the
/// `Active` one and a `Draft` property after it fail at the same step, and
/// all three reports keep that violation.
#[test]
fn known_and_active_failures_at_one_boundary() {
    const KNOWN: PropertyId = PropertyId::new("test/known-zero");
    const ACTIVE: PropertyId = PropertyId::new("test/active-zero");
    const DRAFT: PropertyId = PropertyId::new("test/draft-zero");
    let world = honest();
    let task_world = world.clone();
    let report = report(
        world,
        vec![
            level_zero(KNOWN, Status::Known { issue: 1523 }),
            level_zero(ACTIVE, Status::Active),
            level_zero(DRAFT, Status::Draft),
        ],
        Assumptions::none(),
        |sim| {
            sim.scheduler()
                .spawn_root("bump", async move { bump(&task_world) });
        },
    );
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("the Active one ended it");
    assert_eq!(violation.property, ACTIVE.as_str());
    for id in [&KNOWN, &ACTIVE, &DRAFT] {
        let violations = &property_report(&report, id).violations;
        assert_eq!(violations.len(), 1, "{id}: {violations:?}");
        assert_eq!(violations[0].step, violation.step, "{id}");
    }
}

/// P1 follow-up: a `Known` property that keeps failing keeps every
/// violation, not only its first, in the run's report. The trace's `end`
/// record leaves them out, so a long run's record stays small, and a replay
/// finds them again.
#[test]
fn every_violation_retained() {
    let world = honest();
    let task_world = world.clone();
    let report = report(
        world,
        vec![level_zero(LEVEL, Status::Known { issue: 1523 })],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
                task::yield_now().await;
                bump(&task_world);
                task::yield_now().await;
                task_world.level.set(0);
                task::yield_now().await;
                bump(&task_world);
            });
        },
    );
    let property = property_report(&report, &LEVEL);
    let actual: Vec<&str> = property
        .violations
        .iter()
        .map(|v| v.actual.as_str())
        .collect();
    assert_eq!(actual, ["level 1", "level 2", "level 1"]);
    assert_eq!(
        outcome(&report, &LEVEL),
        PropertyOutcome::Violated {
            witness: property.violations[0].witness.clone()
        }
    );
    let recorded = serde_json::to_value(property).expect("a report serializes");
    assert_eq!(recorded.get("violations"), None, "{recorded}");
}

/// Fails twice at every step where the counter is odd.
fn twice_when_odd(cx: &mut CheckCtx<'_, World>) -> Verdict {
    let level = cx.view();
    if level.is_multiple_of(2) {
        return Verdict::Holds;
    }
    cx.record_violation("an even level", format!("second look at level {level}"));
    cx.violated("an even level", format!("level {level}"))
}

/// P1 follow-up: one check can report several violations at one boundary,
/// such as two punished connections. The verdict's comes first.
#[test]
fn one_check_reports_several_violations() {
    let world = honest();
    let task_world = world.clone();
    let report = report(
        world,
        vec![Property {
            check: twice_when_odd,
            ..level(Status::Known { issue: 1523 })
        }],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
            });
        },
    );
    let violations = &property_report(&report, &LEVEL).violations;
    let actual: Vec<&str> = violations.iter().map(|v| v.actual.as_str()).collect();
    assert_eq!(actual, ["level 1", "second look at level 1"]);
    assert!(violations.iter().all(|v| v.step == violations[0].step));
}

/// Reports missing evidence once the counter is set.
fn evidence_when_set(cx: &mut CheckCtx<'_, World>) -> Verdict {
    if cx.view() == 0 {
        return Verdict::Holds;
    }
    cx.missing_evidence("no label for the judged frame");
    Verdict::NotApplicable
}

/// P1 follow-up: evidence a declared source lacks fails the run as a
/// harness failure, never `Held` or a silent `NotApplicable`.
#[test]
fn missing_evidence_fails_the_harness() {
    let world = honest();
    let task_world = world.clone();
    let report = report(
        world,
        vec![Property {
            check: evidence_when_set,
            ..level(Status::Active)
        }],
        Assumptions::none(),
        |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
            });
        },
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(
        report.end.harness,
        Some(HarnessFailure::MissingEvidence {
            property: LEVEL.to_string(),
            step: 1,
            detail: "no label for the judged frame".to_owned(),
        })
    );
}

/// A frame of the `level` row, labelled `label`.
fn level_frame(label: Label) -> LabelledFrame {
    LabelledFrame {
        family: "test",
        version: 1,
        row: "level",
        direction: Direction::ToNode,
        label,
        outcome: LabelOutcome::Unobserved,
    }
}

/// Labels one frame per bump: `Conformant` for the first, and `OverMargin`
/// after.
fn label_each_bump(cx: &mut CheckCtx<'_, World>) -> Verdict {
    match cx.view() {
        0 => Verdict::NotApplicable,
        1 => {
            cx.label(level_frame(Label::Conformant));
            Verdict::Holds
        }
        _ => {
            cx.label(level_frame(Label::OverMargin));
            Verdict::Holds
        }
    }
}

/// P3a: a frame a check labels reaches the next checker as a
/// `CheckEvent::Label`, after the check's lifecycle events and in step
/// order.
#[test]
fn labelled_frames_reach_the_next_checker() {
    let world = honest();
    let task_world = world.clone();
    let property = Property {
        check: label_each_bump,
        ..level(Status::Active)
    };
    let (_, _, recorder) = run_with(
        world,
        vec![property],
        Assumptions::none(),
        Recorder::default(),
        |sim| {
            sim.scheduler().spawn_root("bump", async move {
                bump(&task_world);
                task::yield_now().await;
                bump(&task_world);
            });
        },
    );
    assert!(matches!(
        recorder.events.first(),
        Some(CheckEvent::Lifecycle {
            to: Lifecycle::Applicable,
            ..
        })
    ));
    let labels: Vec<Label> = recorder
        .labelled()
        .iter()
        .map(|frame| frame.label)
        .collect();
    assert_eq!(labels.first(), Some(&Label::Conformant));
    assert!(labels.len() >= 2, "{labels:?}");
    assert!(labels[1..].iter().all(|label| *label == Label::OverMargin));
    assert!(recorder.events.iter().all(|event| match event {
        CheckEvent::Label { property, .. } | CheckEvent::Lifecycle { property, .. } =>
            *property == LEVEL,
        _ => false,
    }));
}
