//! P1a: the property registry and `RegistryChecker`, over real runs of
//! small tasks (properties.md, P1.1, P1.2 and P1.4).
#![cfg(zakura_sim)]

mod support;

use std::{cell::Cell, rc::Rc, time::Duration};

use support::{config, picks, run};
use zakura_rt::{sync::Mutex, task, time};
use zakura_scheduler::Op;
use zakura_sim::{
    fake::FakeStorage,
    observe::Exit,
    property::{
        Assume, AssumeRecord, Assumptions, CheckCtx, CheckEvent, HasView, Kind, Lifecycle,
        LivenessBound, ObligationKey, Property, PropertyId, Registry, RegistryChecker,
        RegistryError, Source, Status, Subject, Tee, Verdict,
    },
    Checker, IdleStatus, Observation, ObservationLog, Outcome, Plan, PropertyOutcome,
    PropertyReport, RunEnd, RunReport, Sim, StepCtx, Violation,
};

/// The state tasks share with the checks.
#[derive(Default)]
struct World {
    /// Whether the scenario's one assumption holds now.
    honest: Cell<bool>,
    /// A counter tasks bump, for safety checks to read.
    level: Cell<u32>,
}

impl HasView for World {
    type View = u32;

    /// The counter.
    fn view(&self) -> u32 {
        self.level.get()
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

/// What these runs observe: tasks, fake storage, and the world.
const OBSERVED: &[Source] = &[Source::Schedule, Source::Storage, Source::View];

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
    run_observing(world, properties, assumptions, OBSERVED, extra, setup)
}

/// [`run_with`], observing `sources`.
fn run_observing<C: Checker>(
    world: Rc<World>,
    properties: Vec<Property<World>>,
    assumptions: Assumptions<World>,
    sources: &[Source],
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
            .map(|event| match event {
                CheckEvent::Lifecycle { to, .. } => *to,
                other => panic!("unexpected event {other:?}"),
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
        vec![PropertyReport {
            property: "test/recorder".to_owned(),
            outcome: PropertyOutcome::NotApplicable,
            holds_since: None,
        }]
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
/// bad ones. It never runs, so it never emits `Applicable`.
#[test]
fn missing_source_not_applicable() {
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
    assert_eq!(checker.missing_sources(&NO_BAD_FRAMES), [Source::Frames]);

    // A harness that observes frames runs it.
    let (report, checker, _) = run_observing(
        honest(),
        vec![frames_property()],
        Assumptions::none(),
        &[Source::Frames, Source::Schedule],
        Recorder::default(),
        |sim| exit_after(sim, "request-p1", Duration::ZERO),
    );
    assert_eq!(
        outcome(&report, &NO_BAD_FRAMES),
        PropertyOutcome::Held { applicable: 1 }
    );
    assert_eq!(checker.missing_sources(&NO_BAD_FRAMES), []);
}
