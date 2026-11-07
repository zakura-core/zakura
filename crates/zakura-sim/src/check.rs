//! Property checks a run calls after each step and at its end.
//!
//! The property registry, [`crate::property::RegistryChecker`], builds on
//! these hooks. M1 ships [`NoChecks`] and [`FnChecker`] for tests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use zakura_scheduler::{SimInstant, Status};

use crate::{
    end::{HarnessFailure, RunEnd},
    observe::Observation,
    property::CheckEvent,
    Enabled, Identity, ObservationLog,
};

/// A property that failed, and the steps its failure depends on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Violation {
    /// The property's ID, such as `sim/no-livelock`.
    pub property: String,
    /// The step the check failed at.
    pub step: u64,
    /// The steps the violation depends on, in order.
    pub witness: Vec<u64>,
    /// What the property expected.
    pub expected: String,
    /// What the run did.
    pub actual: String,
}

/// One property's outcome over a whole run (properties.md, Verdicts and
/// obligations).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum PropertyOutcome {
    /// `applicable` evaluations applied, none failed, and no obligation is
    /// open.
    Held {
        /// How many evaluations applied.
        applicable: u32,
    },
    /// No evaluation applied.
    NotApplicable,
    /// An obligation was still open at run end, within its bound, the
    /// property's assumptions never held, or a harness failure ended the
    /// run.
    Inconclusive {
        /// Which of the two.
        reason: String,
    },
    /// Some evaluation failed, or an obligation's bound expired.
    Violated {
        /// The steps the violation depends on.
        witness: Vec<u64>,
    },
}

impl PropertyOutcome {
    /// This outcome once a harness failure ended the run: one that held or
    /// never applied is inconclusive, since the run's evidence may be
    /// incomplete. A violation found before the failure stands.
    pub(crate) fn after_harness_failure(self) -> Self {
        match self {
            PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable => {
                PropertyOutcome::Inconclusive {
                    reason: crate::property::Inconclusive::HarnessFailed.to_string(),
                }
            }
            outcome => outcome,
        }
    }
}

/// A property's ID and its run outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyReport {
    /// The property's ID.
    pub property: String,
    /// Its outcome.
    pub outcome: PropertyOutcome,
    /// The step after which all the property's assumptions held. `None`
    /// until properties declare assumptions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holds_since: Option<u64>,
    /// Why the harness doesn't run the property: the excluded source it
    /// needs and the exclusion's reason. Set only with `NotApplicable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excluded: Option<String>,
    /// Each quantity's largest value over the run, as the property's check
    /// measured it, such as the most bytes a resource bound's allowance
    /// held. Empty for a property that measures none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peaks: BTreeMap<String, u64>,
    /// Every violation, in the order the run found them. `Violated`'s
    /// witness is the first one's. A `Known` or `Draft` property can fail
    /// again after its first violation, and one step can expire several
    /// obligations, so the first alone can hide the rest.
    ///
    /// The trace's `end` record leaves them out: their number grows with the
    /// run, and a record has a size limit. A replay of the trace finds every
    /// one again, in `ReplayReport::properties`.
    #[serde(skip)]
    pub violations: Vec<Violation>,
}

impl PropertyReport {
    /// A report of `outcome` for `property`, with no `holds_since`, peaks,
    /// exclusion, or retained violations.
    pub fn new(property: impl Into<String>, outcome: PropertyOutcome) -> Self {
        PropertyReport {
            property: property.into(),
            outcome,
            holds_since: None,
            excluded: None,
            peaks: BTreeMap::new(),
            violations: Vec::new(),
        }
    }
}

/// Which part of the run a step belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Before the step budget runs out. Every action can be enabled.
    Live,
    /// After the budget, when only ready tasks run.
    Drain,
}

/// What a checker sees after one step.
#[derive(Clone, Copy, Debug)]
pub struct StepCtx<'a> {
    /// The step, from 1.
    pub step: u64,
    /// The step's pick.
    pub pick: &'a Identity,
    /// The enabled set the pick came from, sorted by identity.
    pub enabled: &'a [Enabled],
    /// The step's phase.
    pub phase: Phase,
    /// Virtual time after the step.
    pub now: SimInstant,
    /// The scheduler's status after the step.
    pub status: Status,
    /// Every observation so far.
    pub log: &'a ObservationLog,
}

impl StepCtx<'_> {
    /// The observations this step appended.
    pub fn observations(&self) -> impl Iterator<Item = &Observation> {
        self.log.of_step(self.step)
    }
}

/// Checks properties as a run goes.
pub trait Checker {
    /// Called after every step. A violation ends the run at once.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation>;

    /// Called once the run has ended, before cleanup, with the complete live
    /// log. Returns each property's outcome.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport>;

    /// Drains the events this checker emitted since the last call, for a
    /// checker that runs after it in a [`crate::property::Tee`].
    fn events(&mut self) -> Vec<CheckEvent> {
        Vec::new()
    }

    /// Receives the events of a checker that runs before this one in a
    /// [`crate::property::Tee`].
    fn observe_events(&mut self, _events: &[CheckEvent]) {}

    /// A harness failure the checker found, such as a source a property
    /// needs that the harness lacks, or a stale view. The run calls it
    /// before the first step and after every step, and ends at the first
    /// failure.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        None
    }

    /// Whether the step loop's livelock limit applies after this step. A
    /// checker returns false while a scenario's `BoundedAdversary`
    /// assumption doesn't hold. The step loop then restarts its count, so
    /// the limit covers only steps taken while the assumption holds.
    fn bounds_livelock(&mut self) -> bool {
        true
    }

    /// The step loop also found `violation`, at the step where this
    /// checker's own violation ends the run. `end.violation` keeps the
    /// checker's, so a checker that reports the step loop's properties
    /// records this one itself.
    fn step_loop_violation(&mut self, _violation: &Violation) {}
}

/// A checker that checks nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoChecks;

impl Checker for NoChecks {
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
}

/// A checker made of two closures, for tests.
pub struct FnChecker<A, E> {
    after: A,
    end: E,
}

impl<A, E> FnChecker<A, E>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
    E: FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>,
{
    /// A checker that calls `after` after each step and `end` at run end.
    pub fn new(after: A, end: E) -> Self {
        FnChecker { after, end }
    }
}

/// A checker that calls `after` after each step and reports no properties.
pub fn after_step<A>(
    after: A,
) -> FnChecker<A, impl FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
{
    FnChecker::new(after, |_: &RunEnd, _: &ObservationLog| Vec::new())
}

impl<A, E> Checker for FnChecker<A, E>
where
    A: FnMut(&StepCtx<'_>) -> Option<Violation>,
    E: FnMut(&RunEnd, &ObservationLog) -> Vec<PropertyReport>,
{
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        (self.after)(cx)
    }

    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        (self.end)(end, log)
    }
}

impl<C: Checker + ?Sized> Checker for Box<C> {
    /// Forwards to the checker.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        (**self).after_step(cx)
    }

    /// Forwards to the checker.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        (**self).at_end(end, log)
    }

    /// Forwards to the checker.
    fn events(&mut self) -> Vec<CheckEvent> {
        (**self).events()
    }

    /// Forwards to the checker.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        (**self).observe_events(events);
    }

    /// Forwards to the checker.
    fn harness_failure(&mut self) -> Option<HarnessFailure> {
        (**self).harness_failure()
    }

    /// Forwards to the checker.
    fn bounds_livelock(&mut self) -> bool {
        (**self).bounds_livelock()
    }

    /// Forwards to the checker.
    fn step_loop_violation(&mut self, violation: &Violation) {
        (**self).step_loop_violation(violation);
    }
}
