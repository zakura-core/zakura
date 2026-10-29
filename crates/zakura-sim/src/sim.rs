//! The step loop.
//!
//! Each step computes the enabled set, lets the [`Plan`] pick
//! one action, runs it, records what happened, and calls the checker. A run
//! ends at a violation, when nothing is enabled, or after its step budget and
//! a bounded drain.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

use zakura_scheduler::{
    At, CleanupReport, ObjectId, RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status, TaskId,
};

use crate::{
    action::{enabled_set, Action, ActionSource, EnabledInputs, Offer, SourceId},
    check::{Checker, Phase, PropertyOutcome, PropertyReport, StepCtx, Violation},
    draws::{self, Choices, Failure, Owner},
    end::{Drain, HarnessFailure, IdleStatus, Outcome, RunEnd},
    evidence::{Checkpoint, EvidenceId, Mark},
    observe::{Exit, Observation},
    record::{
        digest, CleanupRecord, EndRecord, InitRecord, OutcomeKind, Record, RecordSink, StepRecord,
        ViolationRecord,
    },
    Identity, ObservationLog, Plan, PlanInput,
};

/// The next run's ID. IDs are never serialized, so their values don't
/// affect replay.
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

/// The property a run violates when it spins at one instant too long.
pub const NO_LIVELOCK: &str = "sim/no-livelock";

/// Limits and settings for one run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunConfig {
    /// The scheduler's workers, task limit and stack size.
    pub scheduler: SchedulerConfig,
    /// Live steps before the drain starts.
    pub budget: u64,
    /// The most steps the drain takes.
    pub drain_bound: u64,
    /// The most steps at one instant before a run counts as a livelock. The
    /// count resets only when the clock moves forward. A run with nothing
    /// enabled ends, so it can't sit at one instant idle.
    pub max_steps_per_instant: u64,
    /// Whether passing `max_steps_per_instant` violates [`NO_LIVELOCK`].
    pub enforce_no_livelock: bool,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig {
            scheduler: SchedulerConfig::default(),
            budget: 100_000,
            drain_bound: 10_000,
            max_steps_per_instant: 100_000,
            enforce_no_livelock: false,
        }
    }
}

/// A harness rule that keeps ready tasks out of the enabled set, so a run
/// can pause a task at a chosen point, as a busy thread would pause it.
///
/// A held task sits out only while something else is enabled. When only
/// held tasks are ready, the clock can still move, as it would for a slow
/// thread. When nothing else is enabled at all, every held task is enabled
/// again, so a hold never ends a run.
pub trait TaskHold {
    /// True if `task`, which resumes at `at`, sits out the next pick. The
    /// run asks after each step's observations, once per ready task.
    fn holds(&self, task: TaskId, at: &At) -> bool;
}

/// One step's pick, for tests and reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pick {
    /// The step, from 1.
    pub step: u64,
    /// The pick's identity.
    pub identity: String,
    /// How many actions were enabled.
    pub options: usize,
    /// The step's phase.
    pub phase: Phase,
}

/// Everything a finished run produced.
#[derive(Debug)]
pub struct RunReport {
    /// How the run ended.
    pub end: RunEnd,
    /// Each property's outcome, from the checker.
    pub properties: Vec<PropertyReport>,
    /// What shutdown cleaned up.
    pub cleanup: CleanupReport,
    /// Every live observation.
    pub log: ObservationLog,
    /// Every step's pick, in order.
    pub picks: Vec<Pick>,
    /// The real choices the run made, as a script that replays them.
    pub resolved: Plan,
}

/// One simulated run.
pub struct Sim {
    /// `None` once shut down.
    sched: Option<Scheduler>,
    /// Shared with the draw callback. See [`draws`].
    choices: Rc<RefCell<Choices>>,
    failure: Failure,
    sources: Vec<Box<dyn ActionSource>>,
    /// Each evidence checkpoint the run holds, with its index among its
    /// kind's, in registration order.
    evidence: Vec<(u64, Checkpoint)>,
    /// This run's ID, unique within the process.
    run_id: u64,
    config: RunConfig,
    /// The harness's task holds.
    holds: Vec<Box<dyn TaskHold>>,
}

impl Sim {
    /// Starts a run at virtual time zero, taking its choices from `plan`.
    ///
    /// The draw callback is installed before anything else, so draws in setup
    /// code go to the plan and the `init` record.
    ///
    /// # Panics
    ///
    /// If this thread already has a run.
    pub fn new(config: RunConfig, plan: PlanInput) -> Self {
        let mut sched = Scheduler::new(config.scheduler, SimInstant::ZERO);
        let choices = Rc::new(RefCell::new(Choices::new(plan)));
        let failure = Failure::default();
        draws::install(&mut sched, &choices, &failure);
        Sim {
            sched: Some(sched),
            choices,
            failure,
            sources: Vec::new(),
            evidence: Vec::new(),
            run_id: NEXT_RUN.fetch_add(1, Ordering::Relaxed),
            config,
            holds: Vec::new(),
        }
    }

    /// Keeps the ready tasks `hold` names out of each step's enabled set,
    /// as [`TaskHold`] describes. A trace replays only with the same holds.
    pub fn hold_tasks(&mut self, hold: Box<dyn TaskHold>) {
        self.holds.push(hold);
    }

    /// The run's scheduler, for setup such as spawning the tasks under test.
    /// Draws in setup code are recorded in the `init` record.
    pub fn scheduler(&mut self) -> &mut Scheduler {
        self.sched_mut()
    }

    /// Adds a source of actions outside the scheduler.
    pub fn add_source(&mut self, source: Box<dyn ActionSource>) -> SourceId {
        self.sources.push(source);
        SourceId(self.sources.len() - 1)
    }

    /// The run's harness failure cell, which a source sets for a failure
    /// it finds while observing. The run checks it after every step.
    pub(crate) fn failure(&self) -> Failure {
        self.failure.clone()
    }

    /// Holds `checkpoint` for an evidence source being created. At setup
    /// and after every step, the run ends with
    /// [`HarnessFailure::SourceEvidence`] unless it names that step and the
    /// run's own log holds exactly the entries it records, however the
    /// source was wrapped or added, or if it never was. Returns the
    /// source's identity, whose index counts the run's earlier sources of
    /// the same kind, and which binds it to this run.
    pub(crate) fn register(&mut self, checkpoint: Checkpoint) -> EvidenceId {
        let index = self
            .evidence
            .iter()
            .filter(|(_, held)| held.kind().same_space(checkpoint.kind()))
            .count();
        let id = EvidenceId {
            run: self.run_id,
            // usize always fits in u64 on supported targets.
            index: index as u64,
        };
        self.evidence.push((id.index, checkpoint));
        id
    }

    /// Runs to the end, then checks the end with `checker`, freezes the log,
    /// and shuts the scheduler down. Emits the run's records to `sink`: `init`,
    /// each step, then `end`. A sink that doesn't want steps
    /// ([`RecordSink::wants_steps`]) gets only `init` and `end`, and the run
    /// skips each step's `obs` digest.
    pub fn run(mut self, checker: &mut dyn Checker, sink: &mut dyn RecordSink) -> RunReport {
        let mut state = State::default();
        state.log.set_run(self.run_id);
        let draws = {
            let mut choices = self.choices.borrow_mut();
            // The sink is harness code, so a draw inside it is a failure.
            choices.set_owner(Owner::Harness);
            choices.take_draws()
        };
        sink.record(Record::Init(InitRecord { draws }));
        // A checker's failure from setup, such as a missing source, ends the
        // run before its first step.
        let end = match self.failure.take().or_else(|| checker.harness_failure()) {
            Some(failure) => state.end_harness(failure),
            None => self.drive(checker, &mut state, sink),
        };
        // A trace replay that ended before its record did diverged.
        let unreached = match end.outcome {
            Outcome::Harness => None,
            _ => self.choices.borrow().decider.finish().err(),
        };
        // A failure the last step's code left behind outranks the outcome,
        // and so do an unreached trace record and evidence that names a step
        // the run didn't take.
        let end = match self
            .failure
            .take()
            .or_else(|| unreached.map(HarnessFailure::Plan))
            .or_else(|| evidence_outside_run(&end))
        {
            Some(failure) => state.end_harness(failure),
            None => end,
        };
        // An evidence source the last step's checker or sink dropped after
        // it observed fails the run before its properties are reported.
        let end = match self.end_evidence(&end, &state) {
            Some(failure) => state.end_harness(failure),
            None => end,
        };
        // The checker is harness code, so the owner stays Harness for it.
        let properties = checker.at_end(&end, &state.log);
        let (properties, bad_report) = checked_reports(properties, end.steps);
        // Read even when a failure is already recorded. One `at_end` or its
        // reports caused still replaces the outcome, as before, and a source
        // `at_end` dropped ends the run only if nothing else did.
        let dropped = self.check_evidence(&state).err();
        let end = match self.failure.take().or(bad_report) {
            Some(failure) => state.end_harness(failure),
            None => end,
        };
        let end = match dropped {
            Some(failure) if end.outcome != Outcome::Harness => state.end_harness(failure),
            _ => end,
        };
        // Last, so it covers every way the run ended at a harness failure,
        // which the checker may have reported before it knew. The report and
        // the end record both take these.
        let properties = match end.outcome {
            Outcome::Harness => properties
                .into_iter()
                .map(|report| PropertyReport {
                    outcome: report.outcome.after_harness_failure(),
                    ..report
                })
                .collect(),
            _ => properties,
        };
        state.log.freeze();
        let cleanup = self.shut_down();
        sink.record(Record::End(end_record(
            &end,
            &properties,
            cleanup,
            &state.picks,
        )));
        RunReport {
            end,
            properties,
            cleanup,
            log: state.log,
            picks: state.picks,
            resolved: self.choices.borrow().decider.resolved(),
        }
    }

    /// The run's scheduler, until shutdown.
    fn sched(&self) -> &Scheduler {
        self.sched
            .as_ref()
            .expect("only run and drop shut the scheduler down, and nothing uses it after")
    }

    /// [`sched`](Self::sched), mutably.
    fn sched_mut(&mut self) -> &mut Scheduler {
        self.sched
            .as_mut()
            .expect("only run and drop shut the scheduler down, and nothing uses it after")
    }

    /// Shuts the scheduler down, once. If it abandoned the run, which it does
    /// only when a task got stuck, or it panicked, which leaves tasks
    /// suspended, the sources are forgotten too, since dropping one could
    /// wait on something a leaked task never released.
    fn shut_down(&mut self) -> CleanupReport {
        let Some(sched) = self.sched.take() else {
            return CleanupReport::default();
        };
        // Cleanup draws take the rotating default, unrecorded.
        self.choices.borrow_mut().set_owner(Owner::Done);
        // Held aside, so a shutdown that unwinds forgets them.
        let sources = std::mem::ManuallyDrop::new(std::mem::take(&mut self.sources));
        let cleanup = sched.shutdown();
        if cleanup.leaked == 0 {
            self.sources = std::mem::ManuallyDrop::into_inner(sources);
        }
        cleanup
    }

    /// Steps until the run ends.
    fn drive(
        &mut self,
        checker: &mut dyn Checker,
        st: &mut State,
        sink: &mut dyn RecordSink,
    ) -> RunEnd {
        // Draws are harness failures except while a step runs.
        self.choices.borrow_mut().set_owner(Owner::Harness);
        // Read once, so a run never emits only some of its steps.
        let record_steps = sink.wants_steps();
        if let Err(failure) = self.observe_sources(st) {
            return st.end_harness(failure);
        }
        let mut ready = match self.ready(st) {
            Ok(ready) => ready,
            Err(failure) => return st.end_harness(failure),
        };
        let mut phase = Phase::Live;
        let mut drained = 0;
        let mut at_instant = 0;
        loop {
            // A run whose last live step left nothing enabled stays live, so
            // it ends idle below rather than at its budget.
            if phase == Phase::Live
                && st.step >= self.config.budget
                && !self.nothing_enabled(&ready)
            {
                phase = Phase::Drain;
            }
            if phase == Phase::Drain && drained >= self.config.drain_bound {
                // Exhausted only if the bound cut off work still waiting.
                return st.end_budget(drained, !ready.is_empty());
            }
            let now = self.sched().now();
            let offers: Vec<Offer> = match phase {
                Phase::Live => self.sources.iter().map(|s| offer(&**s, now)).collect(),
                Phase::Drain => Vec::new(),
            };
            // A source that drew while offering, here or in the idle check
            // above, ends the run before anything else runs.
            if let Some(failure) = self.failure.take() {
                return st.end_harness(failure);
            }
            // Held tasks sit out, unless nothing else is enabled. With only
            // held tasks ready, the clock can move, as for a slow thread.
            let free: Option<Vec<(Identity, TaskId)>> = (!st.held.is_empty()).then(|| {
                ready
                    .iter()
                    .filter(|(_, task)| !st.held.contains(task))
                    .cloned()
                    .collect()
            });
            let enabled = match enabled_set(&EnabledInputs {
                phase,
                now,
                ready: free.as_deref().unwrap_or(&ready),
                next_deadline: self.sched().next_deadline(),
                offers: &offers,
            })
            .and_then(|enabled| {
                if enabled.is_empty() && free.is_some() {
                    enabled_set(&EnabledInputs {
                        phase,
                        now,
                        ready: &ready,
                        next_deadline: self.sched().next_deadline(),
                        offers: &offers,
                    })
                } else {
                    Ok(enabled)
                }
            }) {
                Ok(enabled) => enabled,
                Err(identity) => {
                    return st.end_harness(HarnessFailure::DuplicateIdentity {
                        identity: identity.to_string(),
                    })
                }
            };
            if enabled.is_empty() {
                return match phase {
                    Phase::Drain => st.end_budget(drained, false),
                    Phase::Live => match idle_status(self.sched().status()) {
                        Ok(status) => st.end(Outcome::Idle { status }),
                        Err(failure) => st.end_harness(failure),
                    },
                };
            }

            let identities: Vec<Identity> = enabled.iter().map(|e| e.identity.clone()).collect();
            let decision = {
                // This borrow ends before any task code runs, so tasks can draw.
                let mut choices = self.choices.borrow_mut();
                match phase {
                    Phase::Live => choices.decider.pick(&identities),
                    Phase::Drain => choices.decider.pick_round_robin(&identities),
                }
            };
            let decision = match decision {
                Ok(decision) => decision,
                Err(error) => return st.end_harness(HarnessFailure::Plan(error)),
            };
            let chosen = &enabled[decision.index];
            st.step += 1;
            if phase == Phase::Drain {
                drained += 1;
            }
            st.log.begin(st.step);
            st.picks.push(Pick {
                step: st.step,
                identity: chosen.identity.to_string(),
                options: enabled.len(),
                phase,
            });
            let owner = match chosen.action {
                Action::RunTask(task) => Owner::Task {
                    task,
                    name: st.names[&task].clone(),
                },
                _ => Owner::NoTask,
            };
            self.choices.borrow_mut().set_owner(owner);
            let executed = self.execute(chosen.action, st);
            let draws = {
                let mut choices = self.choices.borrow_mut();
                choices.set_owner(Owner::Harness);
                choices.take_draws()
            };
            let observed = match chosen.action {
                // `execute` observed right after the task ran, and nothing
                // has run since.
                Action::RunTask(_) => executed,
                _ => executed.and_then(|()| self.observe_sources(st)),
            };

            let next = self.ready(st);
            if record_steps {
                sink.record(Record::Step(StepRecord {
                    step: st.step,
                    pick: chosen.identity.to_string(),
                    key: None,
                    options: enabled.len(),
                    by: decision.by,
                    offset: decision.offset,
                    draws,
                    obs: Some(digest(st.log.of_step(st.step))),
                }));
            }
            if let Some(failure) = self.failure.take() {
                return st.end_harness(failure);
            }
            // Ends before the checker, which would read incomplete evidence.
            if let Err(failure) = observed {
                return st.end_harness(failure);
            }
            ready = match next {
                Ok(ready) => ready,
                Err(failure) => return st.end_harness(failure),
            };
            // Only a clock that moved ends the instant. An advance to now, or
            // a step where tasks wait while a source acts, doesn't.
            if self.sched().now() > now {
                at_instant = 0;
            } else {
                at_instant += 1;
            }
            let cx = StepCtx {
                step: st.step,
                pick: &chosen.identity,
                enabled: &enabled,
                phase,
                now: self.sched().now(),
                status: self.sched().status(),
                log: &st.log,
            };
            let violation = checker.after_step(&cx);
            if let Some(failure) = self.failure.take().or_else(|| checker.harness_failure()) {
                return st.end_harness(failure);
            }
            // Steps taken while the checker suspends the limit don't count.
            if !checker.bounds_livelock() {
                at_instant = 0;
            }
            // Checked even when the checker found a violation, so a spin at
            // the same step still reaches the checker.
            let spinning = phase == Phase::Live
                && self.config.enforce_no_livelock
                && at_instant > self.config.max_steps_per_instant
                && !self.nothing_enabled(&ready);
            let livelock = spinning.then(|| Violation {
                property: NO_LIVELOCK.to_owned(),
                step: st.step,
                witness: vec![st.step],
                expected: format!(
                    "at most {} steps before the clock moves",
                    self.config.max_steps_per_instant
                ),
                actual: format!("{at_instant} steps at {} ns", now.since_start().as_nanos()),
            });
            let violation = match (violation, livelock) {
                (Some(violation), Some(livelock)) => {
                    checker.step_loop_violation(&livelock);
                    Some(violation)
                }
                (violation, livelock) => violation.or(livelock),
            };
            if let Some(violation) = violation {
                let mut end = st.end(Outcome::Violation);
                end.violation = Some(violation);
                if phase == Phase::Drain {
                    end.drain = Some(Drain {
                        steps: drained,
                        exhausted: false,
                    });
                }
                return end;
            }
        }
    }

    /// True if no action is enabled now, so the run is about to end idle. A
    /// run that ends this way isn't spinning, however many steps its last
    /// instant took.
    fn nothing_enabled(&self, ready: &[(Identity, TaskId)]) -> bool {
        let now = self.sched().now();
        let offers: Vec<Offer> = self.sources.iter().map(|s| offer(&**s, now)).collect();
        enabled_set(&EnabledInputs {
            phase: Phase::Live,
            now,
            ready,
            next_deadline: self.sched().next_deadline(),
            offers: &offers,
        })
        .is_ok_and(|enabled| enabled.is_empty())
    }

    /// Lets every source append what happened inside it to the current
    /// step, then requires every evidence source's checkpoint to name it.
    /// A source that fails doesn't stop the later ones observing, so a
    /// failure one of them records in the run's failure cell, such as a
    /// reactor port panic, still outranks it. The first failure is returned.
    fn observe_sources(&mut self, st: &mut State) -> Result<(), HarnessFailure> {
        let mut failed = None;
        for (index, source) in self.sources.iter_mut().enumerate() {
            if let Err(detail) = source.observe(&mut st.log) {
                failed.get_or_insert(HarnessFailure::SourceEvidence {
                    source: format!("source {index}"),
                    detail,
                });
            }
        }
        match failed {
            Some(failure) => Err(failure),
            None => self.check_evidence(st),
        }
    }

    /// The evidence failure that ends a run which `end` didn't already end
    /// at a harness failure, if any of its evidence sources was dropped, or skipped
    /// a boundary, after its last observation. Checked before the log
    /// freezes, so the run's own shutdown, after it, never fails here.
    fn end_evidence(&self, end: &RunEnd, st: &State) -> Option<HarnessFailure> {
        match end.outcome {
            Outcome::Harness => None,
            _ => self.check_evidence(st).err(),
        }
    }

    /// Requires every evidence checkpoint to name the current step, with
    /// its publisher healthy and the run's own log holding exactly what it
    /// records.
    fn check_evidence(&self, st: &State) -> Result<(), HarnessFailure> {
        for (instance, checkpoint) in &self.evidence {
            let kind = checkpoint.kind();
            let words = kind.words();
            let detail = match checkpoint.get() {
                Mark::NotObserved => {
                    "NotObserved: the setup observation never completed".to_owned()
                }
                Mark::Dropped => {
                    format!("Dropped: the {} was dropped during the run", words.source)
                }
                Mark::Observed(delivered) if delivered.step != st.step => format!(
                    "BoundarySkipped: the last observation was at step {}, not {}",
                    delivered.step, st.step
                ),
                Mark::Observed(delivered) => {
                    // The publisher is read live, since activity after the
                    // last observation leaves the mark as it was.
                    let live = checkpoint.live();
                    let last = live.as_ref().map_or(0, |live| live.last_seq());
                    // Exactly what the source delivered, payloads and order
                    // included, so the checker reads what the source checked.
                    let logged = kind.entries(st.log.of_step(st.step), *instance);
                    if live.as_ref().is_some_and(|live| live.refused_outside_run()) {
                        format!("RefusedOutsideRun: {}", words.refused)
                    } else if let Some(health) = live.as_ref().and_then(|live| live.health()) {
                        health.to_owned()
                    } else if last > delivered.through_seq {
                        format!(
                            "Unobserved: the {} published {} {}..={last} after its last \
                             observation",
                            words.publisher,
                            words.entries,
                            delivered.through_seq + 1
                        )
                    } else if logged.iter().copied().eq(&delivered.observations) {
                        continue;
                    } else {
                        let differs = logged
                            .iter()
                            .zip(&delivered.observations)
                            .position(|(logged, sent)| *logged != sent)
                            .unwrap_or(logged.len().min(delivered.observations.len()));
                        format!(
                            "Undelivered: at step {} the run log holds {} {} entries and \
                             {} delivered {}, first differing at entry {differs}",
                            st.step,
                            logged.len(),
                            words.source,
                            words.source,
                            delivered.observations.len()
                        )
                    }
                }
            };
            return Err(HarnessFailure::SourceEvidence {
                source: kind.source(*instance),
                detail,
            });
        }
        Ok(())
    }

    /// Runs `action` and records what it did. A task's outcome is pushed
    /// after the sources are drained, so what it made a source do comes
    /// first in the step.
    fn execute(&mut self, action: Action, st: &mut State) -> Result<(), HarnessFailure> {
        match action {
            Action::RunTask(task) => {
                let name = st.names[&task].clone();
                let outcome = self.sched_mut().run(task);
                self.observe_sources(st)?;
                let obs = match outcome {
                    RunOutcome::Reached(at) => Observation::TaskReached {
                        task: name,
                        at: at.to_string(),
                        object: at.object.map(|object| self.object_name(st, object)),
                        site: at.site.map(|s| format!("{}:{}", s.file(), s.line())),
                    },
                    RunOutcome::Waiting => Observation::TaskWaiting { task: name },
                    RunOutcome::Parked(_) => Observation::TaskParked { task: name },
                    RunOutcome::Completed => exited(name, Exit::Completed),
                    RunOutcome::Cancelled => exited(name, Exit::Cancelled),
                    RunOutcome::Panicked(_) => exited(name, Exit::Panicked),
                };
                st.log.push(obs);
            }
            Action::AdvanceTime(to) => {
                let fired = self.sched_mut().advance_to(to).len();
                st.log.push(Observation::TimeAdvanced {
                    // u64 nanoseconds cover 584 years of virtual time.
                    to_ns: u64::try_from(to.since_start().as_nanos()).unwrap_or(u64::MAX),
                    fired,
                });
            }
            Action::Source { source, local } => self.sources[source.0].execute(local, &mut st.log),
        }
        Ok(())
    }

    /// `object` as `<creator>:object#<n>`. The creator is the creating task's
    /// name, or `setup` outside any task. A name survives unrelated code
    /// changes, and a `TaskId` doesn't.
    fn object_name(&self, st: &State, object: ObjectId) -> String {
        let creator = match object.creator {
            // A task makes objects only while it runs, so it was offered and
            // named first.
            Some(task) => st
                .names
                .get(&task)
                .cloned()
                .unwrap_or_else(|| self.sched().task_name(task)),
            None => "setup".to_owned(),
        };
        format!("{creator}:object#{}", object.ordinal)
    }

    /// The scheduler's ready tasks with their identities. Records each task
    /// seen for the first time as spawned, and which ones the harness holds.
    fn ready(&self, st: &mut State) -> Result<Vec<(Identity, TaskId)>, HarnessFailure> {
        let ready = self.sched().ready();
        let mut out = Vec::with_capacity(ready.len());
        st.held.clear();
        for r in ready {
            if self.holds.iter().any(|hold| hold.holds(r.task, &r.at)) {
                st.held.insert(r.task);
            }
            let name = match st.names.get(&r.task) {
                Some(name) => name.clone(),
                None => {
                    // A task's name never changes, so it's read once.
                    let name = self.sched().task_name(r.task);
                    if name.contains(':') {
                        return Err(HarnessFailure::TaskNameHasColon { name });
                    }
                    st.log.push(Observation::TaskSpawned { task: name.clone() });
                    st.names.insert(r.task, name.clone());
                    name
                }
            };
            out.push((Identity::task(name, r.at), r.task));
        }
        Ok(out)
    }
}

/// The loop's running state.
#[derive(Default)]
struct State {
    step: u64,
    names: BTreeMap<TaskId, String>,
    /// The ready tasks the harness holds this step.
    held: BTreeSet<TaskId>,
    log: ObservationLog,
    picks: Vec<Pick>,
}

impl State {
    /// An end with `outcome` and nothing else.
    fn end(&self, outcome: Outcome) -> RunEnd {
        RunEnd {
            outcome,
            steps: self.step,
            violation: None,
            drain: None,
            harness: None,
        }
    }

    /// An end after a budget and a drain of `steps`.
    fn end_budget(&self, steps: u64, exhausted: bool) -> RunEnd {
        RunEnd {
            drain: Some(Drain { steps, exhausted }),
            ..self.end(Outcome::Budget)
        }
    }

    /// An end at a harness failure.
    fn end_harness(&self, failure: HarnessFailure) -> RunEnd {
        RunEnd {
            harness: Some(failure),
            ..self.end(Outcome::Harness)
        }
    }
}

/// What `source` offers at `now`.
fn offer(source: &dyn ActionSource, now: SimInstant) -> Offer {
    let mut offer = Offer::default();
    source.enabled(now, &mut offer.actions);
    source.boundaries(now, &mut offer.boundaries);
    offer
}

/// A task-exit observation.
fn exited(task: String, exit: Exit) -> Observation {
    Observation::TaskExited { task, exit }
}

/// The idle status for a scheduler status with nothing enabled.
fn idle_status(status: Status) -> Result<IdleStatus, HarnessFailure> {
    match status {
        Status::Quiescent => Ok(IdleStatus::Quiescent),
        Status::Deadlock => Ok(IdleStatus::Deadlock),
        Status::WorkerStarved => Ok(IdleStatus::WorkerStarved),
        Status::WaitingOnExternal => Ok(IdleStatus::WaitingOnExternal),
        Status::Running => Err(HarnessFailure::StuckWithoutAction { status: "running" }),
        Status::WaitingOnTime => Err(HarnessFailure::StuckWithoutAction {
            status: "waiting-on-time",
        }),
    }
}

/// The failure for a violation that names a step the run didn't take. The
/// `end` record keys each evidence step by its pick, so it needs real steps.
fn evidence_outside_run(end: &RunEnd) -> Option<HarnessFailure> {
    let violation = end.violation.as_ref()?;
    std::iter::once(violation.step)
        .chain(violation.witness.iter().copied())
        .find(|step| !(1..=end.steps).contains(step))
        .map(|step| HarnessFailure::EvidenceOutsideRun {
            property: violation.property.clone(),
            step,
        })
}

/// `reports` with each step the run didn't take removed from their
/// violations' evidence and `holds_since`, so the `end` record stays
/// readable, and the failure for the first one removed.
fn checked_reports(
    mut reports: Vec<PropertyReport>,
    steps: u64,
) -> (Vec<PropertyReport>, Option<HarnessFailure>) {
    let in_run = |step: &u64| (1..=steps).contains(step);
    let mut failure = None;
    for report in &mut reports {
        // 0 means the assumptions held from the start.
        if let Some(step) = report.holds_since.filter(|&step| step > steps) {
            failure.get_or_insert_with(|| HarnessFailure::HoldsSinceOutsideRun {
                property: report.property.clone(),
                step,
            });
            report.holds_since = None;
        }
        if let PropertyOutcome::Violated { witness } = &mut report.outcome {
            if let Some(&step) = witness.iter().find(|step| !in_run(step)) {
                failure.get_or_insert_with(|| HarnessFailure::EvidenceOutsideRun {
                    property: report.property.clone(),
                    step,
                });
            }
            witness.retain(in_run);
        }
        // A violation at a step the run didn't take is dropped whole.
        report.violations.retain_mut(|violation| {
            let mut steps = std::iter::once(&violation.step).chain(&violation.witness);
            if let Some(&step) = steps.find(|step| !in_run(step)) {
                failure.get_or_insert_with(|| HarnessFailure::EvidenceOutsideRun {
                    property: report.property.clone(),
                    step,
                });
            }
            violation.witness.retain(in_run);
            in_run(&violation.step)
        });
    }
    (reports, failure)
}

/// The `end` record for a run that ended with `end`, with `picks` its steps'
/// picks in order.
fn end_record(
    end: &RunEnd,
    properties: &[PropertyReport],
    cleanup: CleanupReport,
    picks: &[Pick],
) -> EndRecord {
    let key = |step: u64| {
        step.checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| picks.get(index))
            .expect("evidence_outside_run ended any run whose evidence names a missing step")
            .identity
            .clone()
    };
    let (outcome, status) = match end.outcome {
        Outcome::Violation => (OutcomeKind::Violation, None),
        Outcome::Budget => (OutcomeKind::Budget, None),
        Outcome::Idle { status } => (OutcomeKind::Idle, Some(status)),
        Outcome::Harness => (OutcomeKind::Harness, None),
    };
    EndRecord {
        outcome,
        status,
        steps: end.steps,
        violation: end
            .violation
            .as_ref()
            .map(|v| ViolationRecord::new(v, v.witness.iter().map(|&step| key(step)).collect())),
        drain: end.drain,
        harness: end.harness.as_ref().map(ToString::to_string),
        cleanup: CleanupRecord::from(cleanup),
        // The record holds what the trace writes, which leaves out each
        // report's violations, so a replay compares like with like.
        properties: properties
            .iter()
            .map(|report| PropertyReport {
                violations: Vec::new(),
                ..report.clone()
            })
            .collect(),
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        // A sim dropped without `run` still shuts down, and forgets its
        // sources in the same cases.
        self.shut_down();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemorySink, NoChecks, Plan};

    /// Draws the run's hash seed from setup code.
    fn draw_in_setup() {
        let _: zakura_rt::collections::HashMap<u8, u8> = Default::default();
    }

    #[test]
    fn held_borrow_is_harness_not_panic() {
        let sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        {
            let _held = sim.choices.borrow_mut();
            draw_in_setup();
        }
        let report = sim.run(&mut NoChecks, &mut MemorySink::default());
        assert_eq!(report.end.outcome, Outcome::Harness);
        assert_eq!(
            report.end.harness,
            Some(HarnessFailure::PlanBorrowedDuringDraw)
        );
    }

    #[test]
    fn callback_does_not_outlive_run() {
        let sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[1; 8]));
        let choices = Rc::downgrade(&sim.choices);
        draw_in_setup();
        sim.run(&mut NoChecks, &mut MemorySink::default());
        assert!(choices.upgrade().is_none(), "the run's choices are gone");

        // The next run on this thread draws from its own plan only.
        let sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        draw_in_setup();
        let mut sink = MemorySink::default();
        sim.run(&mut NoChecks, &mut sink);
        let Record::Init(init) = &sink.records[0] else {
            panic!("the first record is init");
        };
        assert_eq!(init.draws.len(), 1);
        assert_eq!(init.draws[0].value.0, 0, "an exhausted seed draw is 0");
    }
}
