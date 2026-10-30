//! A tapped reactor's private state as a component (M6a-3, rule 3 of the
//! observation contract).
//!
//! The reactor's state lives inside its own task, so nothing reads it at a
//! boundary. The reactor reports it instead: `Settled` with a snapshot
//! before each wait that can't change it, and `Resumed` first after. The
//! markers alone don't make it current, since a turn that skipped its
//! `Resumed` would leave the old snapshot looking settled. So at each
//! boundary, once the step's task outcome is logged, [`ReactorLife`] also
//! asks where the reactor's own task stopped:
//!
//! * At an await, its last report must be a `Settled`, and its snapshot is
//!   current.
//! * At a preemption point or a lock, it's current only if this step's
//!   last report is a `Settled`. Otherwise it's in transition.
//! * Exited, it ends: seen with its final snapshot after `Settled{last}`,
//!   unsettled if it was cancelled or panicked, and a harness failure if it
//!   completed without its final snapshot.
//! * If its task didn't run, nothing it owns changed.
//!
//! The reactor is bound to the task that ran when it reported
//! `Resumed{arm: start}`, by that task's scheduler name, which is unique
//! in a run. Until then it's `Initializing`.

use std::{cell::RefCell, rc::Rc};

use super::{
    tap::ReactorLink,
    view::{HeaderSyncView, Reading},
};
use crate::{
    observe::{Exit, ReactorFact, ReactorSnapshot},
    property::{Component, ComponentKind, Read},
    HarnessFailure, Observation, ObservationLog,
};

/// The reactor's last marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Marker {
    /// `Settled{turn}`.
    Settled(u64),
    /// `Resumed{turn}`.
    Resumed(u64),
}

/// How the bound task stopped this step, if it ran.
enum Stop<'l> {
    /// At an await.
    Waiting,
    /// At a preemption point or a lock.
    Point,
    /// It exited.
    Exited(&'l Exit),
}

/// One tapped reactor's fold: its markers, its bound task and its
/// component.
pub struct ReactorLife {
    link: ReactorLink,
    /// The view whose reactor component this fold also writes, for the
    /// node under test.
    view: Option<Rc<RefCell<HeaderSyncView>>>,
    component: Component<Reading>,
    /// The task it's bound to, by scheduler name.
    task: Option<String>,
    marker: Option<Marker>,
    /// The latest settled snapshot.
    snapshot: Option<ReactorSnapshot>,
    /// Whether `Settled{last}` was logged.
    finished: bool,
    /// Whether its task's exit was logged.
    exited: bool,
    /// Whether its last read was in transition.
    in_transition: bool,
}

impl ReactorLife {
    /// A fold for `link`'s reactor, writing `view`'s reactor component too,
    /// if it's given.
    pub(crate) fn new(link: ReactorLink, view: Option<Rc<RefCell<HeaderSyncView>>>) -> Self {
        ReactorLife {
            link,
            view,
            component: Component::new(ComponentKind::Reactor),
            task: None,
            marker: None,
            snapshot: None,
            finished: false,
            exited: false,
            in_transition: false,
        }
    }

    /// The reactor's component, as of the last boundary.
    pub fn component(&self) -> &Component<Reading> {
        &self.component
    }

    /// The task it's bound to, once it started.
    pub fn task(&self) -> Option<&str> {
        self.task.as_deref()
    }

    /// Folds `step`'s reactor rows and its task outcome, then reads the
    /// component at its boundary.
    pub(crate) fn settle(&mut self, log: &ObservationLog) -> Result<(), HarnessFailure> {
        let step = log.step();
        self.fold(step, log)
            .map_err(|detail| HarnessFailure::SourceEvidence {
                source: self.link.source.clone(),
                detail,
            })
    }

    /// [`settle`](Self::settle), failing with a detail.
    fn fold(&mut self, step: u64, log: &ObservationLog) -> Result<(), String> {
        let instance = self.link.instance;
        let facts: Vec<&ReactorFact> = log
            .of_step(step)
            .filter_map(|obs| match obs {
                Observation::Reactor {
                    instance: index,
                    event,
                    ..
                } if *index == instance => Some(event),
                _ => None,
            })
            .collect();
        // `Sim` logs a task step's outcome last, before this runs.
        let outcome = log
            .of_step(step)
            .filter(|obs| obs.ran_task().is_some())
            .last();
        let ran = outcome.and_then(Observation::ran_task);
        if !facts.is_empty() && self.exited {
            return Err(format!(
                "AfterExit: the reactor reported at step {step} after its task exited"
            ));
        }
        if let (Some(task), false) = (&self.task, facts.is_empty()) {
            if ran != Some(task.as_str()) {
                return Err(format!(
                    "ForeignTask: the reactor bound to {task} reported at step {step} from {ran:?}"
                ));
            }
        }
        let reported = !facts.is_empty();
        let mut started = false;
        let mut settled_last_in_step = false;
        for fact in facts {
            settled_last_in_step = false;
            match fact {
                ReactorFact::Settled {
                    turn,
                    last,
                    snapshot,
                } => {
                    match self.marker {
                        None if *turn == 0 && !*last => self.register()?,
                        None => {
                            return Err(format!(
                                "NoInitialSnapshot: the reactor's first marker, at step {step}, \
                                 is Settled{{turn: {turn}}}"
                            ))
                        }
                        Some(Marker::Resumed(resumed)) if resumed == *turn => {}
                        Some(marker) => {
                            return Err(format!(
                                "TurnGap: Settled{{turn: {turn}}} at step {step} follows {marker:?}"
                            ))
                        }
                    }
                    match snapshot {
                        Some(snapshot) => self.snapshot = Some((**snapshot).clone()),
                        None if self.snapshot.is_some() => {}
                        None => {
                            return Err(format!(
                                "NoInitialSnapshot: Settled{{turn: {turn}}} at step {step} \
                                 repeats a snapshot never logged"
                            ))
                        }
                    }
                    self.marker = Some(Marker::Settled(*turn));
                    self.finished |= *last;
                    settled_last_in_step = true;
                }
                ReactorFact::Resumed { turn, arm } => {
                    match self.marker {
                        Some(Marker::Settled(settled))
                            if !self.finished && *turn == settled + 1 => {}
                        None => {
                            return Err(format!(
                                "NoInitialSnapshot: the reactor's first marker, at step {step}, \
                                 is Resumed{{turn: {turn}}}"
                            ))
                        }
                        Some(marker) => {
                            return Err(format!(
                                "TurnGap: Resumed{{turn: {turn}}} at step {step} follows \
                                 {marker:?}{}",
                                if self.finished {
                                    ", the final Settled"
                                } else {
                                    ""
                                }
                            ))
                        }
                    }
                    if arm == "start" {
                        if *turn != 1 {
                            return Err(format!(
                                "TurnGap: the reactor started at step {step} in turn {turn}"
                            ));
                        }
                        started = true;
                    }
                    self.marker = Some(Marker::Resumed(*turn));
                }
                // The build's own effects, such as a startup repair's
                // dispatch, come before its snapshot.
                _ if self.marker.is_none() => {}
                _ if self.finished => {
                    return Err(format!(
                        "AfterFinal: the reactor reported {fact:?} at step {step} after its \
                         final Settled"
                    ))
                }
                _ => {}
            }
        }
        if started {
            let Some(task) = ran else {
                return Err(format!(
                    "ForeignTask: the reactor started at step {step} outside any task"
                ));
            };
            self.task = Some(task.to_owned());
            self.exists(step)?;
        }
        let stop = match outcome {
            Some(obs) if ran.is_some() && ran == self.task.as_deref() => match obs {
                Observation::TaskWaiting { .. } => Some(Stop::Waiting),
                Observation::TaskExited { exit, .. } => Some(Stop::Exited(exit)),
                _ => Some(Stop::Point),
            },
            _ => None,
        };
        if let Some(Stop::Exited(_)) = stop {
            self.exited = true;
        }
        if let Some(detail) = self.link.take_unpaired() {
            match (&stop, reported) {
                // Teardown drops pending calls' futures, whose receipts no
                // call end follows.
                _ if self.finished || self.exited => {}
                // The reactor carries on, so it lost the call's end.
                (Some(Stop::Waiting), _) | (_, true) => return Err(detail),
                // Mid-teardown, or not yet run: its next report decides.
                _ => self.link.defer_unpaired(detail),
            }
        }
        if let Some(Stop::Exited(exit)) = stop {
            if !self.finished {
                return self.unfinished_exit(step, exit);
            }
        }
        if !self.component.live() {
            // Unregistered, or ended at its final `Settled`.
            return Ok(());
        }
        let current = || {
            Read::Current(Reading::Reactor(
                self.snapshot
                    .clone()
                    .expect("a registered reactor logged its initial snapshot"),
            ))
        };
        let held = || Read::Held {
            holder: self.task.clone().unwrap_or_default(),
        };
        let read = match stop {
            _ if self.finished => {
                // Its final snapshot is its last state, whatever it drops
                // on the way out.
                let last = self.snapshot.clone().map(Reading::Reactor);
                return self.end(step, last);
            }
            Some(Stop::Waiting) if matches!(self.marker, Some(Marker::Settled(_))) => current(),
            Some(Stop::Waiting) => {
                return Err(format!(
                    "UnsettledWait: the reactor waited at step {step} after {:?} with no \
                     Settled",
                    self.marker
                ))
            }
            Some(Stop::Point) if settled_last_in_step => current(),
            Some(Stop::Point) => held(),
            Some(Stop::Exited(_)) => unreachable!("an unfinished exit returned above"),
            // Its observer went with no final `Settled`, which its task's
            // outcome will explain.
            None if self.link.unfinished_drop() => held(),
            // Its task didn't run, so nothing it owns changed.
            None if self.in_transition => held(),
            None => current(),
        };
        self.in_transition = matches!(read, Read::Held { .. });
        self.read(step, read)
    }

    /// Ends the reactor after its task exited at `step` with no final
    /// `Settled`: unsettled if it was cancelled, or panicked with its
    /// observer kept elsewhere, and a failure otherwise.
    fn unfinished_exit(&mut self, step: u64, exit: &Exit) -> Result<(), String> {
        let gone = self.link.unfinished_drop();
        match exit {
            Exit::Cancelled => self.end(step, None),
            Exit::Panicked if !gone => self.end(step, None),
            Exit::Panicked => Err(REACTOR_GONE.to_owned()),
            Exit::Completed if gone => Err(REACTOR_GONE.to_owned()),
            Exit::Completed => Err(format!(
                "NoFinalSnapshot: the reactor's task completed at step {step} with no \
                 Settled{{last}}"
            )),
        }
    }

    /// Registers its read, in its view too.
    fn register(&mut self) -> Result<(), String> {
        self.component.register()?;
        self.with_view(|view| view.register(ComponentKind::Reactor));
        Ok(())
    }

    /// Records that it exists at `step`.
    fn exists(&mut self, step: u64) -> Result<(), String> {
        self.component.exists(step)?;
        self.with_view(|view| view.exists(ComponentKind::Reactor, step));
        Ok(())
    }

    /// Folds `read` at `step`'s boundary.
    fn read(&mut self, step: u64, read: Read<Reading>) -> Result<(), String> {
        self.component.read(step, read.clone())?;
        self.with_view(|view| view.read(ComponentKind::Reactor, step, read));
        Ok(())
    }

    /// Ends it at `step`'s boundary.
    fn end(&mut self, step: u64, state: Option<Reading>) -> Result<(), String> {
        if !self.component.live() {
            return Ok(());
        }
        self.component.end(step, state.clone())?;
        self.with_view(|view| view.end(ComponentKind::Reactor, step, state));
        Ok(())
    }

    /// Runs `f` on its view, if it writes one.
    fn with_view(&self, f: impl FnOnce(&mut HeaderSyncView)) {
        if let Some(view) = &self.view {
            f(&mut view.borrow_mut());
        }
    }
}

/// The failure for a reactor whose task exited after it dropped its
/// observer with no final `Settled`.
const REACTOR_GONE: &str =
    "ReactorGone: the tapped reactor dropped its observer during the run with no final Settled";

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use zakura_network::zakura::{
        observe::{ObserverSlot, ReactorEvent, ReactorSnapshot, ResumeArm},
        spawn_header_sync_reactor,
    };
    use zakura_node_services::header_chain::Port;
    use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

    use super::*;
    use crate::{
        harness::{
            evidence_hub::EvidenceHub,
            header_node::{
                startup, tap::ReactorTap, Chain, DispatchFlag, Effects, HeaderNode, Policy,
                PolicySource, Topology,
            },
            header_sync::SourceManifest,
        },
        property::{
            Assumptions, CheckCtx, Ended, HasView, Kind, Property, PropertyId, Registry,
            RegistryChecker, Source, Sources, State, Status, Verdict,
        },
        Checker, HarnessFailure, MemorySink, Plan, PropertyReport, RunConfig, RunEnd, RunReport,
        Sim, StepCtx, Violation,
    };

    /// A component state's name, for comparing runs.
    fn name<T>(state: State<T>) -> &'static str {
        match state {
            State::Initializing => "initializing",
            State::Current { .. } => "current",
            State::InTransition { .. } => "in-transition",
            State::Ended(Ended::Seen { .. }) => "ended-seen",
            State::Ended(Ended::Unsettled) => "ended-unsettled",
        }
    }

    /// Records each listed fold's reactor state after every step, and the
    /// end it gets in `at_end`.
    #[derive(Default)]
    struct States {
        lives: Vec<Rc<RefCell<ReactorLife>>>,
        /// Per step, each life's state.
        seen: Vec<(u64, Vec<&'static str>)>,
        end: Option<RunEnd>,
    }

    impl Checker for States {
        fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
            let states = self
                .lives
                .iter()
                .map(|life| name(life.borrow().component().state()))
                .collect();
            self.seen.push((cx.step, states));
            None
        }

        fn at_end(&mut self, end: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
            self.end = Some(end.clone());
            Vec::new()
        }
    }

    /// A run with one tapped reactor, folded into a storage-tier view.
    struct Fixture {
        sim: Sim,
        hub: EvidenceHub,
        view: Rc<RefCell<HeaderSyncView>>,
        life: Rc<RefCell<ReactorLife>>,
        /// The reactor's tap, which [`Fixture::run`] adds plainly unless a
        /// test took it.
        tap: Option<ReactorTap>,
        port: Arc<ControlledPort>,
    }

    impl Fixture {
        /// A run of at most `budget` steps, and the reactor's slot.
        fn new(budget: u64) -> (Self, ObserverSlot) {
            let mut sim = Sim::new(
                RunConfig {
                    budget,
                    drain_bound: 0,
                    ..RunConfig::default()
                },
                Plan::from_fuzz(&[]),
            );
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let (slot, tap) = ReactorTap::new(&mut sim, &port, "node");
            let view = Rc::new(RefCell::new(HeaderSyncView::new(SourceManifest::storage())));
            let mut hub = EvidenceHub::new();
            hub.owners(view.clone());
            let life = hub.reactor(tap.link(), Some(view.clone()));
            let fixture = Fixture {
                sim,
                hub,
                view,
                life,
                tap: Some(tap),
                port,
            };
            (fixture, slot)
        }

        /// Runs it under `checker`, with the hub added last.
        fn run(mut self, checker: &mut dyn Checker) -> (RunReport, Rc<RefCell<ReactorLife>>) {
            if let Some(tap) = self.tap {
                self.sim.add_source(Box::new(tap));
            }
            self.sim.add_source(Box::new(self.hub));
            let report = self.sim.run(checker, &mut MemorySink::default());
            drop(self.port);
            (report, self.life)
        }
    }

    /// `Settled{turn}` with an empty snapshot.
    fn settled(turn: u64, last: bool) -> ReactorEvent {
        ReactorEvent::Settled {
            turn,
            last,
            snapshot: Box::<ReactorSnapshot>::default(),
        }
    }

    /// `Resumed{turn, arm}`.
    fn resumed(turn: u64, arm: ResumeArm) -> ReactorEvent {
        ReactorEvent::Resumed { turn, arm }
    }

    /// A preemption point, as the reactor's mid-turn locks and atomics are.
    fn point() {
        let atomic = zakura_rt::sync::atomic::AtomicU64::new(0);
        atomic.load(zakura_rt::sync::atomic::Ordering::SeqCst);
    }

    /// A fake reactor's first turn, which settles before an await: the
    /// build's snapshot, then turn 1.
    fn start(slot: &ObserverSlot) {
        slot.emit(resumed(1, ResumeArm::Start));
        slot.emit(settled(1, false));
    }

    /// The source label of the fixture's tap.
    const TAP: &str = "reactor:node#0";

    thread_local! {
        /// What [`check_reactor`] got from `require` at each step: the
        /// reactor's unhandled calls, or `None`.
        static REQUIRED: RefCell<Vec<(u64, Option<Vec<u64>>)>> = const { RefCell::new(Vec::new()) };
    }

    /// Holds whenever `require` hands out the reactor's state, recording
    /// what it got in [`REQUIRED`].
    fn check_reactor(cx: &mut CheckCtx<'_, Owned>) -> Verdict {
        let view = cx.view();
        let view = view.borrow();
        let reactor = view
            .component(ComponentKind::Reactor)
            .expect("the tier has the reactor");
        let got = crate::property::require(cx, reactor).map(|available| match available {
            crate::property::Available::Current(Reading::Reactor(snapshot))
            | crate::property::Available::Ended {
                state: Reading::Reactor(snapshot),
                ..
            } => snapshot.unhandled_calls.clone(),
        });
        let verdict = if got.is_some() {
            Verdict::Holds
        } else {
            Verdict::NotApplicable
        };
        REQUIRED.with(|required| required.borrow_mut().push((cx.step, got)));
        verdict
    }

    /// A checker running one property that needs the reactor, through
    /// [`check_reactor`], on `view`.
    fn requiring(view: &Rc<RefCell<HeaderSyncView>>) -> RegistryChecker<Owned> {
        let property = Property {
            id: PropertyId::new("test/reactor"),
            claim: "a test claim on the reactor",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Component(ComponentKind::Reactor)],
            status: Status::Active,
            check: check_reactor,
            witness: None,
        };
        let sources = SourceManifest::storage().sources(Sources::default());
        RegistryChecker::new(
            Registry::new(vec![property]).expect("one property"),
            Rc::new(Owned(view.clone())),
            Assumptions::none(),
            &sources,
        )
    }

    /// A reactor that settles with call 1 unhandled and waits, then
    /// resumes with no `Resumed`, handles call 1 in private and stops at
    /// points until the run ends. `require` hands out the settled snapshot
    /// while the reactor waits, and nothing at any boundary of the turn
    /// that changed it, because its task stopped at a point with no
    /// `Settled` after its last report.
    ///
    /// This fails if markers alone make the reactor current.
    #[test]
    fn a_skipped_resume_never_hands_out_the_old_snapshot() {
        let (mut fixture, slot) = Fixture::new(6);
        slot.emit(settled(0, false));
        let unhandled = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let private = unhandled.clone();
        fixture.sim.scheduler().spawn_root("reactor", async move {
            slot.emit(resumed(1, ResumeArm::Start));
            private.store(1, std::sync::atomic::Ordering::Relaxed);
            slot.emit(ReactorEvent::Settled {
                turn: 1,
                last: false,
                snapshot: Box::new(ReactorSnapshot {
                    unhandled_calls: vec![1],
                    ..ReactorSnapshot::default()
                }),
            });
            zakura_rt::task::yield_now().await;
            // The skipped `Resumed`: the turn handles call 1 silently.
            private.store(0, std::sync::atomic::Ordering::Relaxed);
            loop {
                point();
            }
        });
        let mut checker = requiring(&fixture.view);
        REQUIRED.with(|required| required.borrow_mut().clear());
        let (report, life) = fixture.run(&mut checker);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert_eq!(life.borrow().task(), Some("reactor.1"));
        assert_eq!(
            unhandled.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the reactor handled the call it settled with"
        );
        let stops = |pred: fn(&Observation) -> bool| -> Vec<u64> {
            report
                .log
                .entries()
                .iter()
                .filter(|entry| pred(&entry.obs))
                .map(|entry| entry.step)
                .collect()
        };
        let reached = stops(
            |obs| matches!(obs, Observation::TaskReached { task, .. } if task == "reactor.1"),
        );
        let waited =
            stops(|obs| matches!(obs, Observation::TaskWaiting { task } if task == "reactor.1"));
        assert!(reached.len() >= 3, "the turn stops at points: {reached:?}");
        assert_eq!(
            report.end.steps,
            *reached.last().unwrap(),
            "the run ends mid-turn"
        );
        assert_eq!(waited.len(), 1, "it waits once, settled");
        let required = REQUIRED.with(|required| required.borrow().clone());
        for (step, got) in &required {
            if *step == waited[0] {
                assert_eq!(got.as_deref(), Some(&[1][..]), "at the wait, step {step}");
            }
            if reached.contains(step) {
                assert_eq!(*got, None, "mid-turn, step {step}");
            }
        }
        assert!(
            required.iter().any(|(step, _)| reached.contains(step)),
            "the check ran mid-turn"
        );
    }

    /// A reactor whose task completes on the run's last live step without
    /// its final `Settled`, while another handle keeps its observer, fails
    /// the run there, before `at_end`, and a property that needs the
    /// reactor ends `harness-failed`.
    ///
    /// This fails if the exit is audited only at a later boundary.
    #[test]
    fn an_exit_on_the_last_step_is_audited() {
        let (mut fixture, slot) = Fixture::new(1_000);
        slot.emit(settled(0, false));
        let kept = slot.clone();
        fixture.sim.scheduler().spawn_root("reactor", async move {
            start(&slot);
            zakura_rt::task::yield_now().await;
            slot.emit(resumed(2, ResumeArm::Shutdown));
        });
        let mut registry = requiring(&fixture.view);
        let mut checker = Recording {
            inner: &mut registry,
            end: None,
        };
        let (report, _) = fixture.run(&mut checker);
        drop(kept);
        let exited = report
            .log
            .entries()
            .iter()
            .find(|entry| matches!(&entry.obs, Observation::TaskExited { task, .. } if task == "reactor.1"))
            .expect("the reactor exits")
            .step;
        assert_eq!(report.end.steps, exited, "it exits on the last live step");
        let failure = |end: &RunEnd| match &end.harness {
            Some(HarnessFailure::SourceEvidence { source, detail }) => {
                source == TAP && detail.starts_with("NoFinalSnapshot")
            }
            _ => false,
        };
        assert!(failure(&report.end), "{:?}", report.end);
        assert!(
            checker.end.as_ref().is_some_and(failure),
            "at_end already has the failure: {:?}",
            checker.end
        );
        assert_eq!(
            report.properties[0].outcome,
            crate::check::PropertyOutcome::Inconclusive {
                reason: "harness-failed".to_owned()
            }
        );
    }

    /// The view, as a harness's.
    struct Owned(Rc<RefCell<HeaderSyncView>>);

    impl HasView for Owned {
        type View = Rc<RefCell<HeaderSyncView>>;

        fn view(&self) -> Self::View {
            self.0.clone()
        }

        fn captured(&self) -> crate::property::Capture {
            self.0.borrow().captured()
        }
    }

    /// A checker that forwards to `inner` and records the end `at_end`
    /// gets.
    struct Recording<'c> {
        inner: &'c mut dyn Checker,
        end: Option<RunEnd>,
    }

    impl Checker for Recording<'_> {
        fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
            self.inner.after_step(cx)
        }

        fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
            self.end = Some(end.clone());
            self.inner.at_end(end, log)
        }

        fn harness_failure(&mut self) -> Option<HarnessFailure> {
            self.inner.harness_failure()
        }
    }

    /// A completed reactor without its final `Settled` fails the run,
    /// `ReactorGone` once its observer went with it and `NoFinalSnapshot`
    /// while another handle keeps it. One with its final `Settled` ends
    /// seen, with no failure.
    #[test]
    fn a_completed_reactor_needs_its_final_snapshot() {
        for (finish, keep, failure) in [
            (false, false, Some("ReactorGone")),
            (false, true, Some("NoFinalSnapshot")),
            (true, false, None),
        ] {
            let (mut fixture, slot) = Fixture::new(1_000);
            slot.emit(settled(0, false));
            let kept = keep.then(|| slot.clone());
            fixture.sim.scheduler().spawn_root("reactor", async move {
                start(&slot);
                zakura_rt::task::yield_now().await;
                slot.emit(resumed(2, ResumeArm::Shutdown));
                if finish {
                    slot.emit(settled(2, true));
                }
            });
            let (report, life) = fixture.run(&mut States::default());
            drop(kept);
            match failure {
                Some(failure) => assert!(
                    matches!(&report.end.harness, Some(HarnessFailure::SourceEvidence { source, detail })
                        if source == TAP && detail.starts_with(failure)),
                    "{failure}: {:?}",
                    report.end
                ),
                None => {
                    assert_eq!(report.end.harness, None, "{:?}", report.end);
                    assert_eq!(name(life.borrow().component().state()), "ended-seen");
                }
            }
        }
    }

    /// The real reactor, cancelled with no other clone of its observer,
    /// once with steps left and once on the run's last live step, ends
    /// unsettled, and the run doesn't fail.
    ///
    /// This fails if the observer's drop is reported before the reactor's
    /// task outcome explains it.
    #[test]
    fn a_cancelled_reactor_ends_unsettled() {
        for after in [3_u32, 0] {
            let (mut fixture, slot) = Fixture::new(1_000);
            let fixture_port: Arc<dyn Port> = fixture.port.clone();
            let header = HeaderFixture::regtest();
            let (mut startup, repairs) =
                startup(&fixture_port, &header, Chain::default().tip(&header));
            // The reactor holds the only clone of its observer.
            startup.observer = slot;
            let (handle, actions, reactor) =
                spawn_header_sync_reactor(startup).expect("a fixture startup is valid");
            fixture.sim.scheduler().spawn_root("killer", async move {
                for _ in 0..8 {
                    zakura_rt::task::yield_now().await;
                }
                reactor.abort();
                if after == 0 {
                    // Nothing runs after the reactor's own cancellation.
                    return;
                }
                assert!(reactor.await.is_err(), "the reactor is cancelled");
                for _ in 0..after {
                    zakura_rt::task::yield_now().await;
                }
            });
            let mut states = States::default();
            let (report, life) = fixture.run(&mut states);
            // Its inputs outlive it, so it ends only by its cancellation.
            drop((handle, actions, repairs));
            assert_eq!(report.end.harness, None, "after {after}: {:?}", report.end);
            let cancelled = report
                .log
                .entries()
                .iter()
                .find(|entry| {
                    matches!(&entry.obs, Observation::TaskExited { task, exit: Exit::Cancelled }
                        if task.starts_with("header_sync_reactor"))
                })
                .expect("the reactor is cancelled")
                .step;
            assert!(life.borrow().task().is_some(), "it started first");
            assert_eq!(name(life.borrow().component().state()), "ended-unsettled");
            if after == 0 {
                assert_eq!(report.end.steps, cancelled, "the last live step");
            } else {
                assert!(report.end.steps > cancelled, "steps follow it");
            }
        }
    }

    /// Two reactors, each tapped and folded on its own, bind to their own
    /// tasks, and one's mid-turn stops leave the other's state alone.
    ///
    /// This fails if a reactor binds by its spawn label, which both share.
    #[test]
    fn each_reactor_is_bound_to_its_own_task() {
        let (mut fixture, slot) = Fixture::new(3_000);
        let header = HeaderFixture::regtest();
        let storage = PolicySource::new(
            &mut fixture.sim,
            fixture.port.clone(),
            Policy::converging(),
            Effects::default(),
        );
        let tap = fixture.tap.take().expect("the fixture has its tap");
        fixture.sim.add_source(Box::new(storage.with_tap(tap)));
        let mirror_port = Arc::new(ControlledPort::new(&header));
        crate::harness::header_node::preload(mirror_port.engine(), &header, &Chain::main(5));
        let mirror_storage = PolicySource::new(
            &mut fixture.sim,
            mirror_port.clone(),
            Policy::converging(),
            Effects::default(),
        );
        let (mirror_slot, mirror_tap) = ReactorTap::new(&mut fixture.sim, &mirror_port, "mirror-0");
        let mirror = fixture.hub.reactor(mirror_tap.link(), None);
        fixture
            .sim
            .add_source(Box::new(mirror_storage.with_tap(mirror_tap)));
        let dispatch = DispatchFlag::default();
        let nodes = vec![
            HeaderNode::spawn(
                &mut fixture.sim,
                "node",
                1,
                fixture.port.clone(),
                &header,
                Chain::default().tip(&header),
                dispatch.clone(),
                Some(slot),
            ),
            HeaderNode::spawn(
                &mut fixture.sim,
                "mirror-0",
                2,
                mirror_port.clone(),
                &header,
                Chain::main(5).tip(&header),
                dispatch,
                Some(mirror_slot),
            ),
        ];
        let topology = Rc::new(Topology::new(nodes, Effects::default()));
        topology.link(1, 0);
        let mut states = States {
            lives: vec![fixture.life.clone(), mirror.clone()],
            ..States::default()
        };
        let (report, node) = fixture.run(&mut states);
        drop(topology);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let (node_task, mirror_task) = (
            node.borrow().task().map(str::to_owned),
            mirror.borrow().task().map(str::to_owned),
        );
        assert!(
            node_task.is_some() && mirror_task.is_some() && node_task != mirror_task,
            "each started on its own task: {node_task:?}, {mirror_task:?}"
        );
        // At each step only one task runs, so the other's state holds.
        let ran = |step: u64| {
            report
                .log
                .of_step(step)
                .find_map(Observation::ran_task)
                .map(str::to_owned)
        };
        let mut transitions = 0;
        for pair in states.seen.windows(2) {
            let ((_, before), (step, after)) = (&pair[0], &pair[1]);
            let ran = ran(*step);
            for (index, task) in [(0, &node_task), (1, &mirror_task)] {
                if ran.as_ref() != task.as_ref() && before[index] != "initializing" {
                    assert_eq!(before[index], after[index], "step {step}, reactor {index}");
                }
            }
            transitions += usize::from(after[0] == "in-transition" && ran == node_task);
        }
        assert!(transitions > 0, "the node stops mid-turn");
    }

    /// How [`repairing_run`] stops the reactor.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Stopping {
        /// Cancels its shutdown token, so it settles a last time.
        Shutdown,
        /// Aborts its task, while the test keeps another clone of its
        /// observer.
        Abort,
    }

    /// The real reactor, tapped and audited, starting at A5 with the VCT
    /// root at height 1 unavailable, so its build dispatches a context
    /// read, which storage holds. A task stops it as `stopping` says.
    fn repairing_run(stopping: Stopping) -> (RunReport, Rc<RefCell<ReactorLife>>) {
        use zakura_header_chain::{VctRootRepairState, VctRootRepairStatus};

        use crate::harness::header_node::policy::{Allow, Hold, OpKind, Select, Trigger};

        let (mut fixture, slot) = Fixture::new(1_000);
        let header = HeaderFixture::regtest();
        crate::harness::header_node::preload(fixture.port.engine(), &header, &Chain::main(5));
        let held = Policy {
            bound_ms: None,
            holds: vec![Hold {
                select: Select {
                    kind: Some(OpKind::VctContext),
                    ..Select::default()
                },
                // Never: the run makes no locator read.
                until: Trigger::Submitted(Select::nth(OpKind::Locator, 1)),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        };
        let storage = PolicySource::new(
            &mut fixture.sim,
            fixture.port.clone(),
            held,
            Effects::default(),
        );
        let tap = fixture.tap.take().expect("the fixture has its tap");
        fixture.sim.add_source(Box::new(storage.with_tap(tap)));
        let port: Arc<dyn Port> = fixture.port.clone();
        let (mut startup, _) = startup(&port, &header, Chain::main(5).tip(&header));
        let (repairs, repairs_rx) = zakura_rt::sync::watch::channel(VctRootRepairStatus {
            state: VctRootRepairState::Unavailable {
                height: zakura_chain::block::Height(1),
            },
            generation: 7,
        });
        startup.vct_root_repairs = Some(repairs_rx);
        let shutdown = startup.shutdown.clone();
        let kept = (stopping == Stopping::Abort).then(|| slot.clone());
        startup.observer = slot;
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup).expect("a fixture startup is valid");
        fixture.sim.scheduler().spawn_root("stopper", async move {
            for _ in 0..8 {
                zakura_rt::task::yield_now().await;
            }
            match stopping {
                Stopping::Shutdown => shutdown.cancel(),
                Stopping::Abort => reactor.abort(),
            }
        });
        let (report, life) = fixture.run(&mut States::default());
        drop((handle, actions, repairs, kept));
        (report, life)
    }

    /// The facts of `report`'s node reactor, with their steps.
    fn reactor_facts(report: &RunReport) -> Vec<(u64, &ReactorFact)> {
        report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Reactor { event, .. } => Some((entry.step, event)),
                _ => None,
            })
            .collect()
    }

    /// A reactor that starts with a VCT repair dispatches its context read
    /// while it's built, before its turn 0 snapshot, which lists the call.
    /// That's a valid start, and the reactor starts and ends as usual.
    ///
    /// This fails if every report before the initial snapshot fails it.
    #[test]
    fn a_startup_repair_reports_before_the_first_snapshot() {
        let (report, life) = repairing_run(Stopping::Shutdown);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let facts = reactor_facts(&report);
        assert!(
            matches!(
                facts[..2],
                [
                    (0, ReactorFact::Dispatched { call: 1, port: crate::observe::ReactorCall::VctContext { .. } }),
                    (0, ReactorFact::Settled { turn: 0, snapshot: Some(ref snapshot), .. }),
                ] if snapshot.unhandled_calls == [1]
            ),
            "the build dispatches, then settles with the call: {:?}",
            &facts[..2]
        );
        assert!(life.borrow().task().is_some(), "the reactor starts");
        assert_eq!(name(life.borrow().component().state()), "ended-seen");
    }

    /// A reactor stopped with a call still pending drops the call's future,
    /// so storage records `CallerDropped` with no call end. That's its
    /// teardown, not a lost event: shut down, it ends seen, and aborted
    /// with its observer kept elsewhere, it ends unsettled, and neither run
    /// fails.
    ///
    /// This fails if the tap fails a teardown receipt before the reactor's
    /// lifecycle explains it.
    #[test]
    fn a_stop_with_a_pending_call_ends_clean() {
        for (stopping, ended) in [
            (Stopping::Shutdown, "ended-seen"),
            (Stopping::Abort, "ended-unsettled"),
        ] {
            let (report, life) = repairing_run(stopping);
            assert_eq!(report.end.harness, None, "{stopping:?}: {:?}", report.end);
            let dropped = report.log.entries().iter().any(|entry| {
                matches!(
                    entry.obs,
                    Observation::Storage {
                        fact: crate::observe::StorageFact::CallerDropped { .. },
                        ..
                    }
                )
            });
            assert!(
                dropped,
                "{stopping:?}: the pending call's future is dropped"
            );
            assert!(
                !reactor_facts(&report)
                    .iter()
                    .any(|(_, fact)| matches!(fact, ReactorFact::CallEnded { .. })),
                "{stopping:?}: no call end follows"
            );
            assert_eq!(
                name(life.borrow().component().state()),
                ended,
                "{stopping:?}"
            );
        }
    }

    /// A pairing failure that isn't a dropped call's receipt fails the run
    /// even in the step that ends the reactor: a call end with no dispatch,
    /// right before the final `Settled`, is `Mispaired`.
    ///
    /// This fails if teardown excuses every pairing failure.
    #[test]
    fn a_final_snapshot_never_hides_a_mispaired_call() {
        use zakura_network::zakura::observe::CallEnd;

        let (mut fixture, slot) = Fixture::new(1_000);
        let storage = PolicySource::new(
            &mut fixture.sim,
            fixture.port.clone(),
            Policy::converging(),
            Effects::default(),
        );
        let tap = fixture.tap.take().expect("the fixture has its tap");
        fixture.sim.add_source(Box::new(storage.with_tap(tap)));
        slot.emit(settled(0, false));
        fixture.sim.scheduler().spawn_root("reactor", async move {
            slot.emit(resumed(1, ResumeArm::Start));
            slot.emit(ReactorEvent::CallEnded {
                call: 99,
                end: CallEnd::Replied,
            });
            slot.emit(settled(1, true));
        });
        let (report, _) = fixture.run(&mut States::default());
        assert!(
            matches!(&report.end.harness, Some(HarnessFailure::SourceEvidence { detail, .. })
                if detail.starts_with("Mispaired: call 99 ended before its dispatch")),
            "{:?}",
            report.end
        );
        assert!(
            matches!(
                reactor_facts(&report).last(),
                Some((_, ReactorFact::Settled { last: true, .. }))
            ),
            "the call end and the final Settled share the failing step"
        );
    }
}
