//! The step loop.
//!
//! Each step computes the enabled set, lets the [`Plan`] pick
//! one action, runs it, records what happened, and calls the checker. A run
//! ends at a violation, when nothing is enabled, or after its step budget and
//! a bounded drain.

use std::collections::BTreeMap;

use zakura_scheduler::{
    CleanupReport, RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status, TaskId,
};

use crate::{
    action::{enabled_set, Action, ActionSource, EnabledInputs, Offer, SourceId},
    check::{Checker, Phase, PropertyReport, StepCtx, Violation},
    end::{Drain, HarnessFailure, IdleStatus, Outcome, RunEnd},
    observe::{Exit, Observation},
    plan::Decider,
    Identity, ObservationLog, Plan, PlanInput,
};

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
    decider: Decider,
    sources: Vec<Box<dyn ActionSource>>,
    config: RunConfig,
}

impl Sim {
    /// Starts a run at virtual time zero, taking its choices from `plan`.
    ///
    /// # Panics
    ///
    /// If this thread already has a run.
    pub fn new(config: RunConfig, plan: PlanInput) -> Self {
        Sim {
            sched: Some(Scheduler::new(config.scheduler, SimInstant::ZERO)),
            decider: Decider::new(plan),
            sources: Vec::new(),
            config,
        }
    }

    /// The run's scheduler, for setup such as spawning the tasks under test.
    pub fn scheduler(&mut self) -> &mut Scheduler {
        self.sched_mut()
    }

    /// Adds a source of actions outside the scheduler.
    pub fn add_source(&mut self, source: Box<dyn ActionSource>) -> SourceId {
        self.sources.push(source);
        SourceId(self.sources.len() - 1)
    }

    /// Runs to the end, then checks the end with `checker`, freezes the log,
    /// and shuts the scheduler down.
    pub fn run(mut self, checker: &mut dyn Checker) -> RunReport {
        let mut state = State::default();
        let end = self.drive(checker, &mut state);
        let properties = checker.at_end(&end, &state.log);
        state.log.freeze();
        let cleanup = self.shut_down();
        RunReport {
            end,
            properties,
            cleanup,
            log: state.log,
            picks: state.picks,
            resolved: self.decider.resolved(),
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
        // Held aside, so a shutdown that unwinds forgets them.
        let sources = std::mem::ManuallyDrop::new(std::mem::take(&mut self.sources));
        let cleanup = sched.shutdown();
        if cleanup.leaked == 0 {
            self.sources = std::mem::ManuallyDrop::into_inner(sources);
        }
        cleanup
    }

    /// Steps until the run ends.
    fn drive(&mut self, checker: &mut dyn Checker, st: &mut State) -> RunEnd {
        let mut ready = match self.ready(st) {
            Ok(ready) => ready,
            Err(failure) => return st.end_harness(failure),
        };
        let mut phase = Phase::Live;
        let mut drained = 0;
        let mut at_instant = 0;
        loop {
            if phase == Phase::Live && st.step >= self.config.budget {
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
            let enabled = match enabled_set(&EnabledInputs {
                phase,
                now,
                ready: &ready,
                next_deadline: self.sched().next_deadline(),
                offers: &offers,
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

            // The decider's borrow ends here, before any task code runs.
            let identities: Vec<Identity> = enabled.iter().map(|e| e.identity.clone()).collect();
            let decision = match phase {
                Phase::Live => match self.decider.pick(&identities) {
                    Ok(decision) => decision,
                    Err(error) => return st.end_harness(HarnessFailure::Plan(error)),
                },
                Phase::Drain => self.decider.pick_round_robin(&identities),
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
            self.execute(chosen.action, st);

            ready = match self.ready(st) {
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
                phase,
                now: self.sched().now(),
                status: self.sched().status(),
                log: &st.log,
            };
            let violation = checker.after_step(&cx).or_else(|| {
                let spinning = phase == Phase::Live
                    && self.config.enforce_no_livelock
                    && at_instant > self.config.max_steps_per_instant
                    && !self.nothing_enabled(&ready);
                spinning.then(|| Violation {
                    property: NO_LIVELOCK.to_owned(),
                    step: st.step,
                    witness: vec![st.step],
                    expected: format!(
                        "at most {} steps before the clock moves",
                        self.config.max_steps_per_instant
                    ),
                    actual: format!("{at_instant} steps at {} ns", now.since_start().as_nanos()),
                })
            });
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

    /// Runs `action` and records what it did.
    fn execute(&mut self, action: Action, st: &mut State) {
        match action {
            Action::RunTask(task) => {
                let name = st.names[&task].clone();
                let obs = match self.sched_mut().run(task) {
                    RunOutcome::Reached(at) => Observation::TaskReached {
                        task: name,
                        at: at.to_string(),
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
    }

    /// The scheduler's ready tasks with their identities. Records each task
    /// seen for the first time as spawned.
    fn ready(&self, st: &mut State) -> Result<Vec<(Identity, TaskId)>, HarnessFailure> {
        let ready = self.sched().ready();
        let mut out = Vec::with_capacity(ready.len());
        for r in ready {
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

impl Drop for Sim {
    fn drop(&mut self) {
        // A sim dropped without `run` still shuts down, and forgets its
        // sources in the same cases.
        self.shut_down();
    }
}
